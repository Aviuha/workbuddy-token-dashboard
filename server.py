#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy Token 用量看板 — 本地实时服务（仅用 Python 标准库）

数据源:
  1) ~/.workbuddy/traces/<pid>/trace_*.json（旧格式）与 trace_*.jsonl（新格式，事件流）
     每次模型调用的 token 明细（模型、输入/输出 token、缓存命中 token）
  2) ~/.workbuddy/workbuddy.db  session_usage 表
     按用户请求记录的积分消耗（credit_json: {conversationRequestId: credits}）

积分时间口径:
  - 新式 requestId（32 位十六进制、01a 前缀）的前 12 位就是该请求创建时刻的
    毫秒时间戳，可直接解出精确时间（与 trace 首 span 互验，误差秒级）。
  - 旧式随机 UUID 无法解时间，回退到所在会话的最近活动时间，标注为近似。
"""
import bisect
import json
import os
import re
import sqlite3
import threading
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))
HOME = os.path.expanduser('~')


def _pick_wb_dir():
    """定位 WorkBuddy 数据目录（跨平台）。

    - 环境变量 WORKBUDDY_HOME 优先（手动指定）
    - 用户级 ~/.workbuddy（macOS / Linux / Windows 的主位置）
    - Windows 备选：ProgramData\\WorkBuddy\\users\\<用户>\\.workbuddy、
      %APPDATA%\\WorkBuddy、%LOCALAPPDATA%\\WorkBuddy
    - 多个候选同时存在时，取「最近有活动」（db 或 traces 目录最新修改）的一个
    """
    env = os.environ.get('WORKBUDDY_HOME')
    if env and os.path.isdir(env):
        return env
    cands = [os.path.join(HOME, '.workbuddy')]
    if os.name == 'nt':
        pd = os.environ.get('PROGRAMDATA')
        user = os.environ.get('USERNAME')
        if pd and user:
            cands.append(os.path.join(pd, 'WorkBuddy', 'users', user, '.workbuddy'))
        ad = os.environ.get('APPDATA')
        if ad:
            cands.append(os.path.join(ad, 'WorkBuddy'))
        ld = os.environ.get('LOCALAPPDATA')
        if ld:
            cands.append(os.path.join(ld, 'WorkBuddy'))
    best, best_m = None, -1.0
    for c in cands:
        if not os.path.isdir(c):
            continue
        m = 0.0
        for probe in (os.path.join(c, 'workbuddy.db'),
                      os.path.join(c, 'traces'),
                      os.path.join(c, 'logs')):
            try:
                m = max(m, os.path.getmtime(probe))
            except OSError:
                pass
        if m > best_m:
            best, best_m = c, m
    return best or cands[0]


WB_DIR = _pick_wb_dir()
TRACES_DIR = os.path.join(WB_DIR, 'traces')
LOGS_DIR = os.path.join(WB_DIR, 'logs')
DB_PATH = os.path.join(WB_DIR, 'workbuddy.db')
# SQLite URI 在 Windows 下需正斜杠，否则含盘符的反斜杠路径无法解析
DB_URI = 'file:' + DB_PATH.replace('\\', '/') + '?mode=ro'
STATIC_DIR = os.path.join(BASE, 'static')
CACHE_FILE = os.path.join(BASE, 'data', 'index-cache.json')
LOG_CACHE_FILE = os.path.join(BASE, 'data', 'log-index-cache.json')
PORT = 8899

LOCAL_TZ = datetime.now().astimezone().tzinfo
CN_TZ = timezone(timedelta(hours=8))          # 北京时间，用于峰谷判定
RID_RE = re.compile(r'"conversationRequestId":\s*"([0-9a-f]{32})"')
QUA_RE = re.compile(r'qua\.end: (\{.*)$')
LOG_LINE_TIME_RE = re.compile(
    r'^\[(\d{1,2})/(\d{1,2})/(\d{4}), (\d{1,2}):(\d{2}):(\d{2})(?:\.\d+)?\s*(AM|PM)')
SCAN_INTERVAL = 1.5      # 索引扫描周期（秒，默认值；实际周期跟随页面刷新设置）
LOG_SCAN_INTERVAL = 15.0  # 日志扫描周期（秒）
CREDIT_INTERVAL = 3.0    # 积分刷新周期（秒）

# 页面刷新设置（毫秒，0 = 暂停），由 /api/data 的 iv 参数更新；
# 服务端采集周期跟随它自动调整，让用户选择的刷新频率真正决定数据新鲜度。
CLIENT_STATE = {'interval_ms': 5000}


def collect_interval():
    """当前生效的服务端采集周期（秒）

    跟随页面刷新设置：约 0.6 倍刷新间隔，限制在 0.6 ~ 5 秒；
    页面暂停刷新时以 5 秒低频采集（持续归档，节省资源）。
    """
    iv = CLIENT_STATE.get('interval_ms', 5000)
    if not iv:
        return 5.0
    return round(max(0.6, min(iv / 1000.0 * 0.6, 5.0)), 2)
RID_TIME_TOLERANCE = 6 * 3600 * 1000   # span 时间与 requestId 时间戳的最大容忍差
TS_MIN, TS_MAX = 1577836800000, 1893456000000   # 2020-01-01 ~ 2030-01-01

# 官方 API 等价价格（人民币 / 百万 tokens）
#   来源仅采用各厂商官方定价文档；官方未公布定价的模型不在此表（该列显示 —）
#   hit  输入·缓存命中  miss 输入·缓存未命中  out 输出
#   peak_x 高峰倍率（DeepSeek 峰谷定价：闲时价为基准，高峰翻倍，其余模型无峰谷）
#   页面「调整价格」按钮可覆盖/新增价格，覆盖项持久化在 data/pricing-overrides.json
PRICING_DEFAULTS = {
    'deepseek-v4.1-flash': {
        'hit': 0.02, 'miss': 1.0, 'out': 4.0, 'peak_x': 2.0, 'kind': 'official',
        'src': 'DeepSeek 官方定价（deepseek-flash）',
    },
    'custom-local:deepseek-flash': {
        'hit': 0.02, 'miss': 1.0, 'out': 4.0, 'peak_x': 2.0, 'kind': 'official',
        'src': 'DeepSeek 官方定价（deepseek-flash）',
    },
    'kimi-k3-1': {
        'hit': 2.0, 'miss': 20.0, 'out': 100.0, 'peak_x': 1.0, 'kind': 'official',
        'src': 'Kimi 开放平台（K3.1 与 K3 同价）',
    },
    'glm-5.3-flash': {
        'hit': 0.23, 'miss': 0.8, 'out': 2.8, 'peak_x': 1.0, 'kind': 'official',
        'src': '智谱开放平台（bigmodel.cn）',
    },
    'glm-5.2': {
        'hit': 2.0, 'miss': 8.0, 'out': 28.0, 'peak_x': 1.0, 'kind': 'official',
        'src': '智谱开放平台（bigmodel.cn）',
    },
    'hy3': {
        'hit': 0.25, 'miss': 1.0, 'out': 4.0, 'peak_x': 1.0, 'kind': 'official',
        'src': '腾讯云官方文档（混元 Hy3）',
    },
    'hy4-preview': {
        'hit': 0.3, 'miss': 6.0, 'out': 18.0, 'peak_x': 1.0, 'kind': 'official',
        'src': '腾讯云官方文档（混元 Hy4 preview）',
    },
    'minimax-m3': {
        'hit': 0.42, 'miss': 2.1, 'out': 8.4, 'peak_x': 1.0, 'kind': 'official',
        'src': 'MiniMax 官方（标准价永久五折后）',
    },
    'step-5-preview': {
        'hit': 0.7, 'miss': 7.0, 'out': 20.0, 'peak_x': 1.0, 'kind': 'official',
        'src': '阶跃星辰官方（缓存命中价 2026-10-14 起为 0.70）',
    },
}

# ---- 价格手动调整（覆盖项持久化到 data/pricing-overrides.json）----
PRICING_OVERRIDES_FILE = os.path.join(BASE, 'data', 'pricing-overrides.json')


def load_pricing_overrides():
    try:
        with open(PRICING_OVERRIDES_FILE, encoding='utf-8') as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


COLOR_RE = re.compile(r'^#[0-9a-fA-F]{6}$')


def sanitize_overrides(ov):
    """只保留合法字段：四项数值（非负、上限 1e6）与颜色（#rrggbb）、模型名长度 ≤64"""
    clean = {}
    if not isinstance(ov, dict):
        return clean
    for mid, patch in ov.items():
        if not isinstance(mid, str) or len(mid) > 64 or not isinstance(patch, dict):
            continue
        p = {}
        for k in ('hit', 'miss', 'out', 'peak_x'):
            if k in patch:
                try:
                    v = float(patch[k])
                    if 0 <= v <= 1e6:
                        p[k] = v
                except (TypeError, ValueError):
                    pass
        c = patch.get('color')
        if isinstance(c, str) and COLOR_RE.match(c.strip()):
            p['color'] = c.strip().lower()
        if p:
            clean[mid] = p
    return clean


def build_pricing(overrides):
    """内置默认价 ⊕ 用户调整（被调整的模型标记为「手动调整」，也可为未定价模型新增）
    纯颜色覆盖不产生价格条目（保持原价/未定价）"""
    pricing = {k: dict(v) for k, v in PRICING_DEFAULTS.items()}
    for mid, patch in (overrides or {}).items():
        price_fields = {k: v for k, v in patch.items() if k in ('hit', 'miss', 'out', 'peak_x')}
        if not price_fields:
            continue
        base = dict(pricing.get(mid) or {'hit': 0.0, 'miss': 0.0, 'out': 0.0, 'peak_x': 1.0})
        base.update(price_fields)
        base['kind'] = 'custom'
        base['src'] = '手动调整的价格'
        pricing[mid] = base
    return pricing


def build_colors(overrides):
    """从覆盖项提取自选颜色 {model: '#rrggbb'}"""
    return {mid: patch['color'] for mid, patch in (overrides or {}).items()
            if isinstance(patch, dict) and patch.get('color')}


PRICING = build_pricing(load_pricing_overrides())
MODEL_COLOR_OVERRIDES = build_colors(load_pricing_overrides())

# ---- 通用设置（持久化到 data/settings.json）：积分套餐折算单价等 ----
SETTINGS_FILE = os.path.join(BASE, 'data', 'settings.json')
DEFAULT_SETTINGS = {'credit_cny': 140.0, 'credit_quota': 9000.0}   # 套餐实付价：140 元 / 9000 积分
SETTINGS_FIELDS = ('credit_cny', 'credit_quota')


def load_settings():
    out = dict(DEFAULT_SETTINGS)
    try:
        with open(SETTINGS_FILE, encoding='utf-8') as fh:
            data = json.load(fh)
        for k in SETTINGS_FIELDS:
            v = data.get(k) if isinstance(data, dict) else None
            if isinstance(v, (int, float)) and 0 < v <= 1e9:
                out[k] = float(v)
    except Exception:
        pass
    return out


def sanitize_settings(data):
    """设置值校验：两个正数（上限 1e9），非法值回落默认"""
    out = dict(DEFAULT_SETTINGS)
    for k in SETTINGS_FIELDS:
        try:
            v = float((data or {}).get(k))
            if 0 < v <= 1e9:
                out[k] = v
        except (TypeError, ValueError):
            pass
    return out


SETTINGS = load_settings()


def is_peak_cn(ms):
    """北京时间高峰时段：周一至周五 9:00-12:00、14:00-18:00（未剔除法定节假日）"""
    dt = datetime.fromtimestamp(ms / 1000, CN_TZ)
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 60 + dt.minute
    return (540 <= hm < 720) or (840 <= hm < 1080)


def parse_iso_ms(s):
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp() * 1000)
    except Exception:
        return None


def rid_time_of(rid):
    """解析新式 requestId 内嵌的毫秒时间戳；失败返回 None"""
    if not rid or len(rid) != 32 or not rid.startswith('01a'):
        return None
    try:
        v = int(rid[:12], 16)
    except Exception:
        return None
    return v if TS_MIN <= v <= TS_MAX else None


# AI 自动审批（security_reviewer）与内容安全复核（content_security_reviewer）等
# 系统调用的 request_type 标记：由平台自动发起、无真实任务归属，不参与任务统计。
AUX_REQUEST_TYPES = {'security_reviewer', 'content_security_reviewer'}

# 不参与看板统计的模型：WorkBuddy 内置轻量路由模型（系统内部调用，非用户实际使用的模型）
EXCLUDED_MODELS = {'wb-lite'}

# 跨源双胞胎配对的时间容差（毫秒）：同一次调用的 trace 与日志记录时间戳一致（实测差 0 秒）
TWIN_TS_TOLERANCE = 10000


def bucket_start_ms(ms, unit):
    dt = datetime.fromtimestamp(ms / 1000, LOCAL_TZ)
    if unit == '5min':
        b = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
    elif unit == 'hour':
        b = dt.replace(minute=0, second=0, microsecond=0)
    elif unit == 'day':
        b = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif unit == 'week':
        b = (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif unit == 'month':
        b = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        b = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(b.timestamp() * 1000)


UNIT_UPGRADE = {'5min': 'hour', 'hour': 'day', 'day': 'week', 'week': 'month', 'month': None}
UNIT_DOWNGRADE = {'month': 'week', 'week': 'day', 'day': 'hour', 'hour': '5min', '5min': None}
MAX_BUCKETS = 400
MIN_BUCKETS = 8


def gen_bucket_starts(start_ms, end_ms, unit):
    """生成 [start, end) 覆盖的桶起点（升序，超上限截断）"""
    starts = []
    cur = bucket_start_ms(start_ms, unit)
    last = bucket_start_ms(max(end_ms - 1, start_ms), unit)
    while cur <= last and len(starts) < MAX_BUCKETS:
        starts.append(cur)
        dt = datetime.fromtimestamp(cur / 1000, LOCAL_TZ)
        if unit == '5min':
            cur += 300000
        elif unit == 'hour':
            cur += 3600000
        elif unit == 'day':
            cur += 86400000
        elif unit == 'week':
            cur += 7 * 86400000
        else:  # month
            y, m = dt.year, dt.month + 1
            if m > 12:
                y, m = y + 1, 1
            cur = int(datetime(y, m, 1, tzinfo=LOCAL_TZ).timestamp() * 1000)
    if not starts:
        starts = [bucket_start_ms(start_ms, unit)]
    return starts


def _extract_generation_rec(sp):
    """从 generation span 提取记录（.json 与 .jsonl 两种格式通用）；无 usage 返回 None"""
    t = parse_iso_ms(sp.get('startedAt'))
    if t is None:
        return None
    to = sp.get('toolOutput')
    if not to:
        return None
    try:
        out = json.loads(to)
    except Exception:
        return None
    items = out if isinstance(out, list) else [out]
    p = c = k = r = 0
    model = None
    resp_id = None
    got = False
    for it in items:
        if not isinstance(it, dict):
            continue
        u = it.get('usage') or {}
        if not u:
            continue
        got = True
        model = model or it.get('model')
        resp_id = resp_id or it.get('id')
        p += int(u.get('prompt_tokens') or 0)
        c += int(u.get('completion_tokens') or 0)
        k += int(((u.get('prompt_tokens_details') or {}).get('cached_tokens')) or 0)
        r += int(((u.get('completion_tokens_details') or {}).get('reasoning_tokens')) or 0)
    if not got:
        return None
    if k > p:
        k = p
    # requestId：仅当 toolInput 被截断时，最后可见的 id 可能来自历史消息，
    # 用「id 内嵌时间戳与 span 时间是否接近」做校验，避免错配。
    rid = None
    ti = sp.get('toolInput') or ''
    cand = RID_RE.findall(ti)
    if cand:
        rid = cand[-1]
        idt = rid_time_of(rid)
        if idt is None or abs(t - idt) > RID_TIME_TOLERANCE:
            rid = None
    return {
        't': t, 'm': model or 'unknown', 'p': p, 'c': c, 'k': k, 'r': r, 'q': rid,
        'i': resp_id, 'd': int(sp.get('duration') or 0) or None,
    }


def _parse_trace_jsonl(path):
    """解析 .jsonl 事件流 trace：每行一个事件，取 span_end 的 generation span"""
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            txt = fh.read()
    except Exception:
        return None
    if '"generation"' not in txt:
        return []
    recs = []
    for ln in txt.split('\n'):
        ln = ln.strip()
        if not ln:
            continue
        try:
            d = json.loads(ln)
        except Exception:
            continue   # 单行不完整（正在追加写入）或损坏，跳过即可
        if not isinstance(d, dict) or d.get('type') != 'span_end':
            continue
        sp = d.get('span')
        if not isinstance(sp, dict) or sp.get('type') != 'generation':
            continue
        rec = _extract_generation_rec(sp)
        if rec:
            recs.append(rec)
    return recs


def parse_trace_file(path):
    """解析单个 trace 文件（.json 旧格式 / .jsonl 事件流新格式），返回记录列表；None 表示暂时不可读"""
    if path.endswith('.jsonl'):
        return _parse_trace_jsonl(path)
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            txt = fh.read()
    except Exception:
        return None
    if '"type": "generation"' not in txt and '"type":"generation"' not in txt:
        return []
    try:
        d = json.loads(txt)
    except Exception:
        return None
    spans = d.get('spans')
    if not isinstance(spans, list):
        return []
    recs = []
    for sp in spans:
        if not isinstance(sp, dict) or sp.get('type') != 'generation':
            continue
        rec = _extract_generation_rec(sp)
        if rec:
            recs.append(rec)
    return recs


class UsageIndex:
    """增量索引 traces 目录"""

    def __init__(self):
        self.lock = threading.Lock()
        self.by_file = {}        # relpath -> {'mtime','size','recs'}
        self.records = []        # 扁平化并按时间排序
        self.last_scan = None
        self.last_scan_ms = None
        self.error_files = 0
        self._load_cache()
        self.scan()

    def _load_cache(self):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            if data.get('v') != 3:
                self.by_file = {}
                return
            self.by_file = data.get('files', {})
        except Exception:
            self.by_file = {}

    def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            tmp = CACHE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as fh:
                json.dump({'v': 3, 'files': self.by_file, 'saved': int(time.time() * 1000)}, fh)
            os.replace(tmp, CACHE_FILE)
        except Exception:
            pass

    def scan(self):
        t0 = time.time()
        try:
            names = os.listdir(TRACES_DIR)
        except Exception:
            names = []
        existing = set()
        changed = 0
        errors = 0
        for name in names:
            sub = os.path.join(TRACES_DIR, name)
            if not os.path.isdir(sub):
                continue
            try:
                sub_files = [f for f in os.listdir(sub)
                             if f.startswith('trace_') and (f.endswith('.json') or f.endswith('.jsonl'))]
            except Exception:
                continue
            for fn in sub_files:
                full = os.path.join(sub, fn)
                rel = os.path.join(name, fn)
                existing.add(rel)
                try:
                    st = os.stat(full)
                except Exception:
                    continue
                old = self.by_file.get(rel)
                if old and old.get('mtime') == st.st_mtime and old.get('size') == st.st_size:
                    continue
                recs = parse_trace_file(full)
                if recs is None:
                    errors += 1
                    continue
                self.by_file[rel] = {'mtime': st.st_mtime, 'size': st.st_size, 'recs': recs}
                changed += 1
        for rel in list(self.by_file):
            if rel not in existing:
                del self.by_file[rel]
                changed += 1
        recs = []
        for info in self.by_file.values():
            recs.extend(info.get('recs') or [])
        recs.sort(key=lambda x: x['t'])
        with self.lock:
            self.records = recs
            self.last_scan = int(time.time() * 1000)
            self.last_scan_ms = int((time.time() - t0) * 1000)
            self.error_files = errors
        if changed:
            self._save_cache()
        return changed


def parse_log_file(path):
    """解析会话日志里的 qua.end 调用上报；返回记录列表，None 表示暂不可读。

    会话日志（~/.workbuddy/logs/<date>/*.log）里每次模型调用都会写一条
    qua.end 质量上报，含 model / prompt_tokens / total_tokens / message_id。
    它与 trace 互为独立信源：部分会话的 trace 未落盘时，日志仍完整。
    """
    recs = []
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            for line in fh:
                if 'qua.end:' not in line:
                    continue
                m = QUA_RE.search(line)
                if not m:
                    continue
                s = m.group(1)
                idx = s.rfind('}')
                try:
                    d = json.loads(s[:idx + 1])
                except Exception:
                    continue
                mid = d.get('message_id')
                t = rid_time_of(mid)          # 响应 ID 前 12 位为毫秒时间戳
                if t is None:
                    lm = LOG_LINE_TIME_RE.match(line)
                    if not lm:
                        continue
                    mo, dd, yy, hh, mi, ss, ap = lm.groups()
                    hh = int(hh) % 12 + (12 if ap == 'PM' else 0)
                    try:
                        t = int(datetime(int(yy), int(mo), int(dd), hh, int(mi), int(ss),
                                         tzinfo=CN_TZ).timestamp() * 1000)
                    except Exception:
                        continue
                try:
                    pt = int(d.get('prompt_tokens') or 0)
                    tt = int(d.get('total_tokens') or 0)
                except Exception:
                    continue
                if tt <= 0 or pt < 0 or tt < pt:
                    continue
                recs.append({'t': t, 'm': d.get('model') or 'unknown',
                             'pt': pt, 'tt': tt, 'mid': mid,
                             'q': d.get('conversation_request_id'),
                             'sid': d.get('conversation_id'),
                             'rt': d.get('request_type') or None,
                             'd': int(d.get('total_cost_ms') or 0) or None})
    except Exception:
        return None
    return recs


class LogIndex:
    """索引会话日志里的 qua.end 上报（作为 trace 的补充数据源）"""

    def __init__(self):
        self.lock = threading.Lock()
        self.by_file = {}
        self.records = []
        self.last_scan = None
        self._load_cache()
        self.scan()

    def _load_cache(self):
        try:
            with open(LOG_CACHE_FILE, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            if data.get('v') != 5:
                self.by_file = {}
                return
            self.by_file = data.get('files', {})
        except Exception:
            self.by_file = {}

    def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(LOG_CACHE_FILE), exist_ok=True)
            tmp = LOG_CACHE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as fh:
                json.dump({'v': 5, 'files': self.by_file, 'saved': int(time.time() * 1000)}, fh)
            os.replace(tmp, LOG_CACHE_FILE)
        except Exception:
            pass

    def scan(self):
        files = []
        try:
            for d in os.listdir(LOGS_DIR):
                dp = os.path.join(LOGS_DIR, d)
                if not os.path.isdir(dp) or not d[:2] == '20':
                    continue
                try:
                    for f in os.listdir(dp):
                        if f.endswith('.log'):
                            files.append(os.path.join(dp, f))
                except Exception:
                    continue
        except Exception:
            files = []
        existing = set()
        changed = 0
        for full in files:
            rel = os.path.relpath(full, LOGS_DIR)
            existing.add(rel)
            try:
                st = os.stat(full)
            except Exception:
                continue
            old = self.by_file.get(rel)
            if old and old.get('mtime') == st.st_mtime and old.get('size') == st.st_size:
                continue
            recs = parse_log_file(full)
            if recs is None:
                continue
            self.by_file[rel] = {'mtime': st.st_mtime, 'size': st.st_size, 'recs': recs}
            changed += 1
        for rel in list(self.by_file):
            if rel not in existing:
                del self.by_file[rel]
                changed += 1
        # 按响应 ID 去重后扁平化
        seen = set()
        recs = []
        for info in self.by_file.values():
            for r in (info.get('recs') or []):
                k = r.get('mid')
                if k:
                    if k in seen:
                        continue
                    seen.add(k)
                recs.append(r)
        recs.sort(key=lambda r: r['t'])
        with self.lock:
            self.records = recs
            self.last_scan = int(time.time() * 1000)
        if changed:
            self._save_cache()


class Archive:
    """本地持久归档（SQLite，append-only）

    WorkBuddy 的 trace 会轮转清理、会话日志只保留数天；本类把每次扫描到的
    调用记录与积分记录永久落库（按唯一键幂等去重），之后统计不再依赖原始
    文件的存续，只需持续增量导入新数据。
    """

    def __init__(self):
        self.path = os.path.join(BASE, 'data', 'archive.db')
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.con = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.con.execute('PRAGMA journal_mode=WAL')
        self.con.execute('PRAGMA synchronous=NORMAL')
        self.lock = threading.Lock()
        with self.lock:
            c = self.con.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS calls (
                uid TEXT PRIMARY KEY, src TEXT, ts INTEGER, model TEXT,
                pt INTEGER, k INTEGER, ct INTEGER, rr INTEGER, rid TEXT)''')
            cols = {row[1] for row in c.execute('PRAGMA table_info(calls)')}
            if 'dur' not in cols:      # v2 迁移：新增调用耗时（毫秒）
                c.execute('ALTER TABLE calls ADD COLUMN dur INTEGER')
            if 'sid' not in cols:      # v3 迁移：新增所属会话（用于按任务合并）
                c.execute('ALTER TABLE calls ADD COLUMN sid TEXT')
            if 'rt' not in cols:       # v4 迁移：新增请求类型（识别 AI 自动审批等系统调用）
                c.execute('ALTER TABLE calls ADD COLUMN rt TEXT')
            c.execute('CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts)')
            c.execute('''CREATE TABLE IF NOT EXISTS credits (
                rid TEXT PRIMARY KEY, cr REAL, sid TEXT, t INTEGER,
                tsrc TEXT, sess_model TEXT, updated INTEGER)''')
            self.con.commit()
            self.known = {row[0] for row in c.execute('SELECT uid FROM calls')}
        self.synced_calls = len(self.known)

    def sync_calls(self, trace_recs, log_recs):
        """把新记录写入归档（幂等，只写内存中未见过的唯一键）"""
        pending = []
        for r in trace_recs:
            uid = 't:' + (r.get('i') or ('%d|%s|%d|%d' % (r['t'], r['m'], r['p'], r['c'])))
            if uid in self.known:
                continue
            self.known.add(uid)
            pending.append((uid, 'trace', r['t'], r['m'], r['p'], r['k'], r['c'], r['r'],
                            r.get('q'), r.get('d'), None, None))
        for r in log_recs:
            uid = 'l:' + (r.get('mid') or ('%d|%s|%d|%d' % (r['t'], r['m'], r['pt'], r['tt'])))
            if uid in self.known:
                continue
            self.known.add(uid)
            pending.append((uid, 'log', r['t'], r['m'], r['pt'], 0, r['tt'] - r['pt'], 0,
                            r.get('q'), r.get('d'), r.get('sid'), r.get('rt')))
        if not pending:
            return 0
        with self.lock:
            self.con.executemany(
                'INSERT OR IGNORE INTO calls (uid, src, ts, model, pt, k, ct, rr, rid, dur, sid, rt) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', pending)
            self.con.commit()
        self.synced_calls += len(pending)
        return len(pending)

    def backfill_durations(self, trace_recs, log_recs):
        """一次性迁移：为已归档但缺耗时的记录回填 dur（trace 与日志双源，幂等）"""
        rows = [(r['d'], 't:' + r['i']) for r in trace_recs
                if r.get('i') and r.get('d')]
        rows += [(r['d'], 'l:' + r['mid']) for r in log_recs
                 if r.get('mid') and r.get('d')]
        if not rows:
            return 0
        with self.lock:
            cur = self.con.executemany(
                'UPDATE calls SET dur = ? WHERE uid = ? AND dur IS NULL', rows)
            self.con.commit()
        return cur.rowcount or 0

    def backfill_sids(self, log_recs):
        """一次性迁移：为已归档但缺会话的日志记录回填 sid（幂等）"""
        rows = [(r['sid'], 'l:' + r['mid']) for r in log_recs
                if r.get('mid') and r.get('sid')]
        if not rows:
            return 0
        with self.lock:
            cur = self.con.executemany(
                'UPDATE calls SET sid = ? WHERE uid = ? AND sid IS NULL', rows)
            self.con.commit()
        return cur.rowcount or 0

    def backfill_rts(self, log_recs):
        """一次性迁移：为已归档但缺请求类型的日志记录回填 rt（幂等）"""
        rows = [(r['rt'], 'l:' + r['mid']) for r in log_recs
                if r.get('mid') and r.get('rt')]
        if not rows:
            return 0
        with self.lock:
            cur = self.con.executemany(
                'UPDATE calls SET rt = ? WHERE uid = ? AND rt IS NULL', rows)
            self.con.commit()
        return cur.rowcount or 0

    def query_calls(self, start_ms, end_ms):
        with self.lock:
            rows = self.con.execute(
                'SELECT uid, src, ts, model, pt, k, ct, rr, rid, dur, sid, rt FROM calls '
                'WHERE ts >= ? AND ts < ? ORDER BY ts', (start_ms, end_ms)).fetchall()
        return [{'i': r[0][2:], 'src': r[1], 't': r[2], 'm': r[3], 'p': r[4], 'k': r[5],
                 'c': r[6], 'r': r[7], 'q': r[8], 'd': r[9], 'sid': r[10],
                 'rt': r[11]} for r in rows]

    def distinct_models(self):
        with self.lock:
            return [r[0] for r in self.con.execute('SELECT DISTINCT model FROM calls')
                    if r[0]]

    def sync_credits(self, rows):
        if not rows:
            return
        with self.lock:
            self.con.executemany(
                '''INSERT INTO credits (rid, cr, sid, t, tsrc, sess_model, updated)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(rid) DO UPDATE SET cr=excluded.cr, t=excluded.t,
                   tsrc=excluded.tsrc, sess_model=excluded.sess_model, updated=excluded.updated''',
                [(r['rid'], r['cr'], r['sid'], r['t'], r['tsrc'],
                  r.get('sess_model'), r.get('upd')) for r in rows])
            self.con.commit()

    def query_credits(self):
        with self.lock:
            rows = self.con.execute(
                'SELECT rid, cr, sid, t, tsrc, sess_model, updated FROM credits').fetchall()
        return [{'rid': r[0], 'cr': r[1], 'sid': r[2], 't': r[3], 'tsrc': r[4],
                 'sess_model': r[5], 'upd': r[6]} for r in rows]


class CreditStore:
    """读取 session_usage 积分记录，并解析时间"""

    def __init__(self):
        self.rows = []
        self.sessions = {}
        self.err = None
        self.refresh()

    def refresh(self):
        try:
            con = sqlite3.connect(DB_URI, uri=True, timeout=3)
            cur = con.cursor()
            sessions = {}
            try:
                for r in cur.execute('SELECT id, model, COALESCE(custom_title, title), created_at, updated_at FROM sessions'):
                    sessions[r[0]] = {'model': r[1], 'title': r[2], 'created': r[3], 'updated': r[4]}
            except Exception:
                pass
            rows = []
            for sid, cj, ua, used, size in cur.execute(
                    'SELECT session_id, credit_json, updated_at, used, size FROM session_usage'):
                if not cj:
                    continue
                try:
                    items = json.loads(cj)
                except Exception:
                    continue
                if not isinstance(items, dict):
                    continue
                sess = sessions.get(sid) or {}
                for rid, cr in items.items():
                    try:
                        cr = float(cr)
                    except Exception:
                        continue
                    idt = rid_time_of(rid)
                    if idt is not None:
                        t, tsrc = idt, 'id'
                    else:
                        t, tsrc = (sess.get('updated') or ua or 0), 'session'
                    rows.append({'rid': rid, 'cr': cr, 'sid': sid, 'upd': ua,
                                 'used': used, 'size': size, 't': int(t), 'tsrc': tsrc,
                                 'sess_model': sess.get('model')})
            con.close()
            self.rows = rows
            self.sessions = sessions
            self.err = None
        except Exception as e:
            self.err = str(e)


BYOK_PREFIX = 'custom-local:'   # 自带 API Key 的模型（走用户自己的 API），调用不消耗 WorkBuddy 积分
# 不消耗积分的模型（平台免费/预览期免费等；经调用级扣费日志核实：
# step-5-preview 在 10-15 / 10-16 的全部调用（236 / 67 个）均无 credit consumed 记录）。
# 与 BYOK 同处理：不参与积分归属，避免积分被误归到它们头上。
FREE_MODELS = {'step-5-preview'}


def is_byok(model):
    return bool(model) and model.startswith(BYOK_PREFIX)


def is_free(model):
    return bool(model) and model in FREE_MODELS


def build_rid_model(records):
    """requestId -> 出现最多的非 BYOK / 非免费模型（用于积分按模型归属）

    自带 API Key 与免费模型的调用不计积分，因此不参与积分归属；
    若某个请求只包含这类调用，则不出现在映射中。
    """
    models = {}
    for r in records:
        q = r.get('q')
        if not q or is_byok(r['m']) or is_free(r['m']):
            continue
        d = models.setdefault(q, {})
        d[r['m']] = d.get(r['m'], 0) + r['p'] + r['c']
    return {q: max(d.items(), key=lambda kv: kv[1])[0] for q, d in models.items()}


def build_sid_model(records):
    """会话 -> 会话内实际主模型（同口径排除 BYOK / 免费模型）。

    用于积分的会话级归属：会话表记录的模型字段可能滞后或不准，
    以会话内实际调用（token 最多者）为准。
    """
    models = {}
    for r in records:
        sid = r.get('sid')
        if not sid or is_byok(r['m']) or is_free(r['m']):
            continue
        d = models.setdefault(sid, {})
        d[r['m']] = d.get(r['m'], 0) + r['p'] + r['c']
    return {sid: max(d.items(), key=lambda kv: kv[1])[0] for sid, d in models.items()}


def credit_model_of(credit, rid_model, sid_model, sessions):
    """积分条目 -> 归属模型；优先请求级、其次会话实际调用、再次会话表记录；无法定位返回「未归属」"""
    m = rid_model.get(credit['rid'])
    if not m and credit.get('sid'):
        m = sid_model.get(credit['sid'])
    if not m:
        sess_m = credit.get('sess_model') or (sessions.get(credit['sid']) or {}).get('model')
        if sess_m and not is_byok(sess_m) and not is_free(sess_m):
            m = sess_m
    return m or '未归属'


def aggregate(index, credit_store, log_index, archive, start_ms, end_ms, unit, now_ms):
    # 归档库为唯一数据源（trace + 日志双源，按唯一键幂等去重后的全集）；
    # 内置路由模型（wb-lite 等）不参与统计，在此统一剔除
    all_recs = [r for r in archive.query_calls(0, now_ms + 86400000)
                if r['m'] not in EXCLUDED_MODELS]
    recs = [r for r in all_recs if start_ms <= r['t'] < end_ms]
    trace_recs = [r for r in recs if r.get('src') != 'log']
    log_recs = [r for r in recs if r.get('src') == 'log']

    # 日志补充：日志里有、trace 缺失的调用。
    # 按 (模型, 输入, 总量) 多重集与 trace 去重；缓存命中量按区间实测命中率估算。
    supplements = []
    if log_recs:
        tp = sum(r['p'] for r in trace_recs)
        tk = sum(r['k'] for r in trace_recs)
        cov_rate = (tk / tp) if tp else 0.9
        avail = Counter((r['m'], r['p'], r['p'] + r['c']) for r in trace_recs)
        for lr in log_recs:
            key = (lr['m'], lr['p'], lr['p'] + lr['c'])
            if avail.get(key, 0) > 0:
                avail[key] -= 1
                continue
            p, c = lr['p'], lr['c']
            if p < 0 or c < 0:
                continue
            supplements.append({'t': lr['t'], 'm': lr['m'], 'p': p, 'c': c,
                                'k': int(p * cov_rate), 'r': 0, 'q': lr.get('q'), 'supp': True,
                                'd': lr.get('d')})
    supp_tokens = sum(r['p'] + r['c'] for r in supplements)
    recs = trace_recs + supplements

    # 桶生成：起点兜底到数据起点（支持「全部」一类超宽范围）；
    # 粒度双向自适应——被截断（太细）自动升级，过少（太粗）自动细一级，
    # 保证图表既不过密也不过于粗糙。
    if all_recs and start_ms < all_recs[0]['t']:
        start_ms = all_recs[0]['t']
    for _ in range(6):
        starts = gen_bucket_starts(start_ms, end_ms, unit)
        last_needed = bucket_start_ms(max(end_ms - 1, start_ms), unit)
        if starts[-1] < last_needed and UNIT_UPGRADE.get(unit):
            unit = UNIT_UPGRADE[unit]
            continue
        if len(starts) < MIN_BUCKETS and UNIT_DOWNGRADE.get(unit):
            unit = UNIT_DOWNGRADE[unit]
            continue
        break
    n_buckets = len(starts)
    first_b = starts[0]

    kpi = {'total': 0, 'prompt': 0, 'cached': 0, 'fresh': 0, 'completion': 0,
           'reasoning': 0, 'calls': 0, 'credits': 0.0, 'credit_reqs': 0, 'cost': 0.0,
           'prompt_cov': 0, 'supp_calls': len(supplements), 'supp_tokens': supp_tokens}
    models = {}
    trend = {}
    type_trend = {'p': [0] * n_buckets, 'k': [0] * n_buckets, 'c': [0] * n_buckets}
    for r in recs:
        bi = bisect.bisect_right(starts, r['t']) - 1
        if bi < 0 or bi >= n_buckets:
            continue
        fresh = r['p'] - r['k']
        kpi['total'] += r['p'] + r['c']
        kpi['prompt'] += r['p']
        kpi['cached'] += r['k']
        kpi['fresh'] += fresh
        kpi['completion'] += r['c']
        kpi['reasoning'] += r['r']
        kpi['calls'] += 1
        if not r.get('supp'):
            kpi['prompt_cov'] += r['p']      # 有缓存明细的输入（命中率分母）
        # 官方 API 等价价格（人民币），DeepSeek 按调用发生时间判定峰谷
        price = PRICING.get(r['m'])
        cost = 0.0
        if price:
            mult = price['peak_x'] if (price['peak_x'] > 1 and is_peak_cn(r['t'])) else 1.0
            cost = (r['k'] * price['hit'] + fresh * price['miss'] + r['c'] * price['out']) * mult / 1e6
            kpi['cost'] += cost
        m = models.setdefault(r['m'], {'m': r['m'], 'calls': 0, 'p': 0, 'k': 0, 'c': 0,
                                       'reason': 0, 'credits': 0.0, 'credit_reqs': 0,
                                       'cost': 0.0, 'first': r['t'], 'last': r['t'], 'p_cov': 0,
                                       'out_dur': 0, 'out_tok': 0})
        m['calls'] += 1
        m['p'] += r['p']
        m['k'] += r['k']
        m['c'] += r['c']
        m['reason'] += r['r']
        m['cost'] += cost
        if not r.get('supp'):
            m['p_cov'] += r['p']
        if r.get('d'):      # 生成速度：仅统计有耗时记录的调用（输出 tokens ÷ 生成时长）
            m['out_dur'] += r['d']
            m['out_tok'] += r['c']
        m['first'] = min(m['first'], r['t'])
        m['last'] = max(m['last'], r['t'])
        tr = trend.setdefault(r['m'], {'p': [0] * n_buckets, 'k': [0] * n_buckets, 'c': [0] * n_buckets})
        tr['p'][bi] += fresh
        tr['k'][bi] += r['k']
        tr['c'][bi] += r['c']
        type_trend['p'][bi] += fresh
        type_trend['k'][bi] += r['k']
        type_trend['c'][bi] += r['c']

    # ---- 积分 ----
    rid_model = build_rid_model(all_recs)
    sid_model = build_sid_model(all_recs)
    credit_rows = archive.query_credits()
    if not credit_rows:
        credit_rows = credit_store.rows
    credit_by_model = {}
    credit_trend = {}
    credits_exact = 0.0
    n_approx = 0
    n_unassigned = 0
    for c in credit_rows:
        t = c['t']
        if not (start_ms <= t < end_ms):
            continue
        kpi['credits'] += c['cr']
        kpi['credit_reqs'] += 1
        if c['tsrc'] == 'id':
            credits_exact += c['cr']
        else:
            n_approx += 1
        m = credit_model_of(c, rid_model, sid_model, credit_store.sessions)
        if m == '未归属':
            n_unassigned += 1
        credit_by_model[m] = credit_by_model.get(m, 0.0) + c['cr']
        if m in models:
            models[m]['credits'] += c['cr']
            models[m]['credit_reqs'] += 1
        bi = bisect.bisect_right(starts, t) - 1
        if 0 <= bi < n_buckets:
            cd = credit_trend.setdefault(m, [0.0] * n_buckets)
            cd[bi] += c['cr']

    # 每请求总 token：同一调用的双源记录（trace/log）先合并——
    # trace 版优先（含缓存明细），rid / 会话 / 请求类型取任一版本非空（trace 常缺这些）。
    # 先按 mid 合并同 ID 双源，再按「内容 + 时间」合并跨 ID 双胞胎（见下）。
    mid_best = {}
    for r in all_recs:
        mid = r.get('i')
        if not mid:
            continue
        cur = mid_best.get(mid)
        if cur is None:
            mid_best[mid] = r
            continue
        keep_trace = r.get('src') == 'trace' and cur.get('src') != 'trace'
        base, other = (r, cur) if keep_trace else (cur, r)
        if ((not base.get('q') and other.get('q')) or
                (not base.get('sid') and other.get('sid')) or
                (not base.get('rt') and other.get('rt'))):
            base = dict(base)
            base['q'] = base.get('q') or other.get('q')
            base['sid'] = base.get('sid') or other.get('sid')
            base['rt'] = base.get('rt') or other.get('rt')
        mid_best[mid] = base

    # 跨 ID 双胞胎合并：部分时期的同一调用在两侧的 ID 格式不同（如自带 Key 调用的
    # UUID 型 response id 与 01a 型 message id），未能按 mid 合并——同一调用会以
    # 日志版（带会话归属、无缓存数据、按全价）与 trace 版（带真实缓存）各计一次。
    # 按「模型 + 输入 + 输出 + 时间邻近」配对：把日志版的请求/会话归属并入 trace 版，
    # 移除日志版，避免任务统计重复计数与价格虚高。
    twin_pool = {}
    for mid, r in mid_best.items():
        if r.get('src') == 'trace':
            twin_pool.setdefault((r['m'], r['p'], r['c']), []).append(mid)
    claimed = set()
    drop_logs = []
    for mid, r in mid_best.items():
        if r.get('src') != 'log':
            continue
        cands = [m2 for m2 in twin_pool.get((r['m'], r['p'], r['c']), ())
                 if m2 not in claimed
                 and abs(mid_best[m2]['t'] - r['t']) <= TWIN_TS_TOLERANCE]
        if not cands:
            continue
        best = min(cands, key=lambda m2: abs(mid_best[m2]['t'] - r['t']))
        claimed.add(best)
        tr = dict(mid_best[best])
        tr['q'] = tr.get('q') or r.get('q')
        tr['sid'] = tr.get('sid') or r.get('sid')
        tr['rt'] = tr.get('rt') or r.get('rt')
        mid_best[best] = tr
        drop_logs.append(mid)
    for mid in drop_logs:
        del mid_best[mid]

    rid_tok = {}
    for r in mid_best.values():
        q = r.get('q')
        if q:
            rid_tok[q] = rid_tok.get(q, 0) + r['p'] + r['c']

    credit_rid = {c['rid']: c for c in credit_rows}
    latest_credits = []
    for c in sorted(credit_rows, key=lambda x: -x['t'])[:8]:
        m = credit_model_of(c, rid_model, sid_model, credit_store.sessions)
        sess = credit_store.sessions.get(c['sid']) or {}
        latest_credits.append({'t': c['t'], 'cr': round(c['cr'], 4), 'm': m,
                               'tsrc': c['tsrc'], 'sid': c['sid'][:8],
                               'title': sess.get('title') or '',
                               'tokens': rid_tok.get(c['rid'], 0)})

    # 任务统计：按会话合并（同一连续任务的多个请求与模型都不拆分），列口径与模型明细一致
    # rid → sid：优先取日志记录的会话归属，辅以积分表
    # 仅统计有真实会话归属的调用：AI 自动审批等系统调用、无法归属的调用均不参与
    rid_sid = {}
    for r in mid_best.values():
        q = r.get('q')
        if q and r.get('sid') and q not in rid_sid:
            rid_sid[q] = r['sid']
    for c in credit_rows:
        rid_sid.setdefault(c['rid'], c['sid'])
    rid_cr = {c['rid']: c['cr'] for c in credit_rows}

    task_agg = {}
    for r in mid_best.values():
        if (r.get('rt') or '') in AUX_REQUEST_TYPES:
            continue                 # AI 自动审批等系统调用：无真实任务归属，不参与任务统计
        q = r.get('q')
        sid = rid_sid.get(q) if q else None
        if not sid and r.get('sid'):
            sid = r['sid']           # 无 rid 但有会话归属的调用，直接按会话归入
        if not sid:
            continue                 # 无法归属会话的调用：忽略（不再生成「未命名会话」行）
        t = task_agg.get(sid)
        if t is None:
            t = task_agg[sid] = {'key': sid, 'calls': 0, 'p': 0, 'k': 0, 'c': 0,
                                 'dur': 0, 'c_dur': 0, 'cost': 0.0,
                                 'first': r['t'], 'last': r['t'],
                                 'models': set(), 'reqs': set()}
        t['calls'] += 1
        t['p'] += r['p']
        t['k'] += r['k']
        t['c'] += r['c']
        if r.get('d'):
            t['dur'] += r['d']
            t['c_dur'] += r['c']
        price = PRICING.get(r['m'])
        if price:
            fresh = r['p'] - r['k']
            mult = price['peak_x'] if (price['peak_x'] > 1 and is_peak_cn(r['t'])) else 1.0
            t['cost'] += (r['k'] * price['hit'] + fresh * price['miss'] + r['c'] * price['out']) * mult / 1e6
        if r['t'] < t['first']:
            t['first'] = r['t']
        if r['t'] > t['last']:
            t['last'] = r['t']
        t['models'].add(r['m'])
        t['reqs'].add(q)

    task_stats = []
    for t in task_agg.values():
        if not (start_ms <= t['last'] < end_ms):
            continue
        key = t['key']
        is_sess = not key.startswith('R:')
        sess = credit_store.sessions.get(key) if is_sess else None
        title = (sess or {}).get('title') or ''
        if not title:
            # 无法命名归属的会话（系统临时 ID、已被清理的老会话等）：忽略，不显示「未命名会话」行
            continue
        credits_sum = sum(rid_cr.get(q, 0.0) for q in t['reqs'])
        task_stats.append({
            'sid': key[:8] if is_sess else key[2:14],
            't': t['last'], 'first': t['first'],
            'title': title,
            'models': sorted(t['models']),
            'reqs': len(t['reqs']),
            'calls': t['calls'],
            'p': t['p'] - t['k'], 'k': t['k'], 'c': t['c'],
            'total': t['p'] + t['c'],
            'speed': round(t['c_dur'] / (t['dur'] / 1000.0), 1) if t['dur'] else None,
            'rate': (t['k'] / t['p']) if t['p'] else 0.0,
            'credits': round(credits_sum, 4) if credits_sum else 0.0,
            'cost': round(t['cost'], 4),
        })
    task_stats.sort(key=lambda x: -x['t'])
    task_stats = task_stats[:300]

    # 最近调用明细
    recent = []
    for r in recs[-40:][::-1]:
        q = r.get('q')
        info = credit_rid.get(q) if q else None
        recent.append({'t': r['t'], 'm': r['m'], 'p': r['p'], 'k': r['k'], 'c': r['c'],
                       'cr': round(info['cr'], 4) if info else None})
        if len(recent) >= 15:
            break

    buckets = list(starts)
    model_list = sorted(models.values(), key=lambda m: -(m['p'] + m['c']))
    if credit_by_model.get('未归属'):
        model_list.append({
            'm': '未归属', 'calls': 0, 'p': 0, 'k': 0, 'c': 0, 'total': 0,
            'reason': 0, 'credits': credit_by_model['未归属'], 'credit_reqs': n_unassigned,
            'cost': 0.0, 'first': None, 'last': None, 'p_cov': 0, 'speed': None,
            'out_dur': 0, 'out_tok': 0,
        })
    credits_total_all = sum(c['cr'] for c in credit_rows)
    n_id = len([c for c in credit_rows if c['tsrc'] == 'id'])

    return {
        'now': now_ms,
        'meta': {
            'range': {'start': start_ms, 'end': end_ms, 'unit': unit, 'buckets': n_buckets},
            'data_from': all_recs[0]['t'] if all_recs else None,
            'data_to': all_recs[-1]['t'] if all_recs else None,
            'records_total': len(all_recs),
            'records_in_range': len(recs),
            'files': len(index.by_file),
            'scan': {'last': index.last_scan, 'ms': index.last_scan_ms, 'errors': index.error_files},
            'collect_interval': collect_interval(),
            'log_supplement': {'n': len(supplements), 'tokens': supp_tokens,
                               'log_files': len(log_index.by_file) if log_index else 0,
                               'log_records': len(log_index.records) if log_index else 0},
            'credit_precision': {'id': n_id, 'session': len(credit_rows) - n_id,
                                 'credits_all': round(credits_total_all, 2)},
            'settings': SETTINGS,
        },
        'kpi': {
            **kpi,
            'cache_rate': (kpi['cached'] / kpi['prompt']) if kpi['prompt'] else 0.0,
            'cost': round(kpi['cost'], 4),
            'credits': round(kpi['credits'], 4),
            'credits_exact': round(credits_exact, 4),
        },
        'trend': {'buckets': buckets, 'models': trend, 'types': type_trend},
        'models': [{
            'm': m['m'], 'calls': m['calls'], 'p': m['p'] - m['k'], 'k': m['k'], 'c': m['c'],
            'total': m['p'] + m['c'], 'reasoning': m['reason'],
            'rate': (m['k'] / m['p']) if m['p'] else 0.0,
            'credits': round(m['credits'], 4), 'credit_reqs': m['credit_reqs'],
            'cost': round(m['cost'], 4) if m['m'] in PRICING else None,
            'price': PRICING.get(m['m']),
            'color': MODEL_COLOR_OVERRIDES.get(m['m']),
            'speed': round(m.get('out_tok', 0) / (m.get('out_dur', 0) / 1000.0), 1) if m.get('out_dur') else None,
            'first': m['first'], 'last': m['last'],
        } for m in model_list],
        'credit_trend': {'buckets': buckets, 'by_model': credit_trend,
                         'by_model_total': {k: round(v, 4) for k, v in credit_by_model.items()}},
        'latest_credits': latest_credits,
        'task_stats': task_stats,
        'recent': recent,
    }


INDEX = None
CREDITS = None
LOGS = None
ARCHIVE = None


def refresh_loop():
    last_credit = 0
    last_log = 0
    while True:
        step = collect_interval()
        try:
            INDEX.scan()
        except Exception:
            pass
        now = time.time()
        if now - last_log > max(step * 4.0, 12.0):
            try:
                LOGS.scan()
            except Exception:
                pass
            last_log = now
        # 汇聚新记录到持久归档（幂等，仅写未见过的新数据）
        try:
            ARCHIVE.sync_calls(INDEX.records, LOGS.records)
        except Exception:
            pass
        if now - last_credit > max(step * 2.0, 2.0):
            try:
                CREDITS.refresh()
                ARCHIVE.sync_credits(CREDITS.rows)
            except Exception:
                pass
            last_credit = now
        time.sleep(step)


MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.ico': 'image/x-icon',
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype='application/json; charset=utf-8'):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False)
        data = body.encode('utf-8') if isinstance(body, str) else body
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def do_GET(self):
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        if u.path == '/favicon.ico':
            self.send_response(204)
            self.end_headers()
            return
        if u.path == '/api/data':
            try:
                now = int(time.time() * 1000)
                start = int(qs.get('start', [str(now - 86400000)])[0])
                end = int(qs.get('end', [str(now)])[0])
                unit = qs.get('unit', ['hour'])[0]
                # 页面刷新设置（毫秒，0 = 暂停）→ 服务端采集周期跟随
                iv_raw = qs.get('iv', [None])[0]
                if iv_raw is not None:
                    try:
                        CLIENT_STATE['interval_ms'] = max(0, int(float(iv_raw)))
                    except (TypeError, ValueError):
                        pass
                if unit not in ('5min', 'hour', 'day', 'week', 'month'):
                    unit = 'hour'
                if end <= start:
                    end = start + 60000
                result = aggregate(INDEX, CREDITS, LOGS, ARCHIVE, start, end, unit, now)
                self._send(200, result)
            except Exception as e:
                self._send(500, {'error': str(e)})
            return
        if u.path == '/api/meta':
            self._send(200, {
                'traces_dir': TRACES_DIR,
                'db': DB_PATH,
                'files': len(INDEX.by_file),
                'records': len(INDEX.records),
                'last_scan': INDEX.last_scan,
                'db_error': CREDITS.err,
            })
            return
        if u.path == '/api/pricing':
            models_list = []
            try:
                models_list = [m for m in ARCHIVE.distinct_models()
                               if m not in EXCLUDED_MODELS]
            except Exception:
                pass
            self._send(200, {
                'defaults': PRICING_DEFAULTS,
                'overrides': load_pricing_overrides(),
                'effective': PRICING,
                'models': models_list,
            })
            return
        if u.path == '/api/settings':
            self._send(200, {'settings': SETTINGS, 'defaults': DEFAULT_SETTINGS})
            return
        path = u.path
        if path == '/':
            path = '/index.html'
        if path.startswith('/static/'):
            rel = path[len('/static/'):]
        else:
            rel = path.lstrip('/')
        full = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not full.startswith(STATIC_DIR) or not os.path.isfile(full):
            self._send(404, {'error': 'not found'})
            return
        ext = os.path.splitext(full)[1]
        try:
            with open(full, 'rb') as fh:
                data = fh.read()
            self._send(200, data, MIME.get(ext, 'application/octet-stream'))
        except Exception:
            self._send(500, {'error': 'read fail'})

    def do_POST(self):
        u = urlparse(self.path)
        try:
            length = int(self.headers.get('Content-Length') or 0)
            raw = self.rfile.read(length) if length else b'{}'
            body = json.loads(raw.decode('utf-8') or '{}')
        except Exception:
            self._send(400, {'error': '请求体不是合法 JSON'})
            return
        if u.path == '/api/pricing':
            global PRICING, MODEL_COLOR_OVERRIDES
            ov = sanitize_overrides(body.get('overrides') or {})
            try:
                os.makedirs(os.path.dirname(PRICING_OVERRIDES_FILE), exist_ok=True)
                tmp = PRICING_OVERRIDES_FILE + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as fh:
                    json.dump(ov, fh, ensure_ascii=False, indent=1)
                os.replace(tmp, PRICING_OVERRIDES_FILE)
            except Exception as e:
                self._send(500, {'error': str(e)})
                return
            PRICING = build_pricing(ov)
            MODEL_COLOR_OVERRIDES = build_colors(ov)
            self._send(200, {'ok': True, 'count': len(ov)})
            return
        if u.path == '/api/settings':
            global SETTINGS
            st = sanitize_settings(body.get('settings') if isinstance(body.get('settings'), dict) else body)
            try:
                os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
                tmp = SETTINGS_FILE + '.tmp'
                with open(tmp, 'w', encoding='utf-8') as fh:
                    json.dump(st, fh, ensure_ascii=False, indent=1)
                os.replace(tmp, SETTINGS_FILE)
            except Exception as e:
                self._send(500, {'error': str(e)})
                return
            SETTINGS = st
            self._send(200, {'ok': True, 'settings': st})
            return
        self._send(404, {'error': 'not found'})


def main():
    global INDEX, CREDITS, LOGS, ARCHIVE
    print('WorkBuddy Token 看板 — 正在建立索引...')
    INDEX = UsageIndex()
    LOGS = LogIndex()
    ARCHIVE = Archive()
    n_new = ARCHIVE.sync_calls(INDEX.records, LOGS.records)
    n_bf = ARCHIVE.backfill_durations(INDEX.records, LOGS.records)   # 历史记录回填耗时（一次性迁移）
    n_bf_sid = ARCHIVE.backfill_sids(LOGS.records)     # 历史记录回填会话（一次性迁移）
    n_bf_rt = ARCHIVE.backfill_rts(LOGS.records)       # 历史记录回填请求类型（一次性迁移）
    CREDITS = CreditStore()
    ARCHIVE.sync_credits(CREDITS.rows)
    print(f'  索引完成: {len(INDEX.by_file)} 个 trace 文件, {len(INDEX.records)} 条调用记录')
    print(f'  日志索引: {len(LOGS.by_file)} 个日志文件, {len(LOGS.records)} 条上报记录')
    print(f'  归档: 本次新入库 {n_new} 条，回填耗时 {n_bf} 条 / 会话 {n_bf_sid} 条 / 类型 {n_bf_rt} 条，'
          f'累计 {ARCHIVE.synced_calls} 条调用 / {len(ARCHIVE.query_credits())} 条积分')
    print(f'  积分记录: {len(CREDITS.rows)} 条' + (f' (DB 错误: {CREDITS.err})' if CREDITS.err else ''))
    t = threading.Thread(target=refresh_loop, daemon=True)
    t.start()
    srv = ThreadingHTTPServer(('127.0.0.1', PORT), Handler)
    print(f'  服务已启动: http://127.0.0.1:{PORT}')
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
