#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy Token 用量看板 — 本地实时服务（仅用 Python 标准库）

数据源:
  1) ~/.workbuddy/traces/<pid>/trace_*.json
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
WB_DIR = os.path.join(HOME, '.workbuddy')
TRACES_DIR = os.path.join(WB_DIR, 'traces')
LOGS_DIR = os.path.join(WB_DIR, 'logs')
DB_PATH = os.path.join(WB_DIR, 'workbuddy.db')
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
PRICING = {
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


def parse_trace_file(path):
    """解析单个 trace 文件，返回记录列表；None 表示暂时不可读（正在写入等）"""
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
        t = parse_iso_ms(sp.get('startedAt'))
        if t is None:
            continue
        to = sp.get('toolOutput')
        if not to:
            continue
        try:
            out = json.loads(to)
        except Exception:
            continue
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
            continue
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
        recs.append({
            't': t, 'm': model or 'unknown', 'p': p, 'c': c, 'k': k, 'r': r, 'q': rid,
            'i': resp_id,
        })
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
            if data.get('v') != 2:
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
                json.dump({'v': 2, 'files': self.by_file, 'saved': int(time.time() * 1000)}, fh)
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
                sub_files = [f for f in os.listdir(sub) if f.startswith('trace_') and f.endswith('.json')]
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
                             'q': d.get('conversation_request_id')})
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
            if data.get('v') != 2:
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
                json.dump({'v': 2, 'files': self.by_file, 'saved': int(time.time() * 1000)}, fh)
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
            pending.append((uid, 'trace', r['t'], r['m'], r['p'], r['k'], r['c'], r['r'], r.get('q')))
        for r in log_recs:
            uid = 'l:' + (r.get('mid') or ('%d|%s|%d|%d' % (r['t'], r['m'], r['pt'], r['tt'])))
            if uid in self.known:
                continue
            self.known.add(uid)
            pending.append((uid, 'log', r['t'], r['m'], r['pt'], 0, r['tt'] - r['pt'], 0, r.get('q')))
        if not pending:
            return 0
        with self.lock:
            self.con.executemany(
                'INSERT OR IGNORE INTO calls (uid, src, ts, model, pt, k, ct, rr, rid) '
                'VALUES (?,?,?,?,?,?,?,?,?)', pending)
            self.con.commit()
        self.synced_calls += len(pending)
        return len(pending)

    def query_calls(self, start_ms, end_ms):
        with self.lock:
            rows = self.con.execute(
                'SELECT src, ts, model, pt, k, ct, rr, rid FROM calls WHERE ts >= ? AND ts < ? ORDER BY ts',
                (start_ms, end_ms)).fetchall()
        return [{'src': r[0], 't': r[1], 'm': r[2], 'p': r[3], 'k': r[4],
                 'c': r[5], 'r': r[6], 'q': r[7]} for r in rows]

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
            con = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True, timeout=3)
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


def is_byok(model):
    return bool(model) and model.startswith(BYOK_PREFIX)


def build_rid_model(records):
    """requestId -> 出现最多的非 BYOK 模型（用于积分按模型归属）

    自带 API Key 的模型调用不计积分，因此不参与积分归属；
    若某个请求只包含 BYOK 调用，则不出现在映射中。
    """
    models = {}
    for r in records:
        q = r.get('q')
        if not q or is_byok(r['m']):
            continue
        d = models.setdefault(q, {})
        d[r['m']] = d.get(r['m'], 0) + r['p'] + r['c']
    return {q: max(d.items(), key=lambda kv: kv[1])[0] for q, d in models.items()}


def credit_model_of(credit, rid_model, sessions):
    """积分条目 -> 归属模型；无法精确定位时返回「未归属」"""
    m = rid_model.get(credit['rid'])
    if not m:
        sess_m = credit.get('sess_model') or (sessions.get(credit['sid']) or {}).get('model')
        if sess_m and not is_byok(sess_m):
            m = sess_m
    return m or '未归属'


def aggregate(index, credit_store, log_index, archive, start_ms, end_ms, unit, now_ms):
    # 归档库为唯一数据源（trace + 日志双源，按唯一键幂等去重后的全集）
    all_recs = archive.query_calls(0, now_ms + 86400000)
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
                                'k': int(p * cov_rate), 'r': 0, 'q': lr.get('q'), 'supp': True})
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
                                       'cost': 0.0, 'first': r['t'], 'last': r['t'], 'p_cov': 0})
        m['calls'] += 1
        m['p'] += r['p']
        m['k'] += r['k']
        m['c'] += r['c']
        m['reason'] += r['r']
        m['cost'] += cost
        if not r.get('supp'):
            m['p_cov'] += r['p']
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
        m = credit_model_of(c, rid_model, credit_store.sessions)
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

    # 最新积分记录（不限范围，展示用）
    credit_rid = {c['rid']: c for c in credit_rows}
    latest_credits = []
    for c in sorted(credit_rows, key=lambda x: -x['t'])[:8]:
        m = credit_model_of(c, rid_model, credit_store.sessions)
        sess = credit_store.sessions.get(c['sid']) or {}
        latest_credits.append({'t': c['t'], 'cr': round(c['cr'], 4), 'm': m,
                               'tsrc': c['tsrc'], 'sid': c['sid'][:8],
                               'title': sess.get('title') or ''})

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
            'cost': 0.0, 'first': None, 'last': None, 'p_cov': 0,
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
            'first': m['first'], 'last': m['last'],
        } for m in model_list],
        'credit_trend': {'buckets': buckets, 'by_model': credit_trend,
                         'by_model_total': {k: round(v, 4) for k, v in credit_by_model.items()}},
        'latest_credits': latest_credits,
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


def main():
    global INDEX, CREDITS, LOGS, ARCHIVE
    print('WorkBuddy Token 看板 — 正在建立索引...')
    INDEX = UsageIndex()
    LOGS = LogIndex()
    ARCHIVE = Archive()
    n_new = ARCHIVE.sync_calls(INDEX.records, LOGS.records)
    CREDITS = CreditStore()
    ARCHIVE.sync_credits(CREDITS.rows)
    print(f'  索引完成: {len(INDEX.by_file)} 个 trace 文件, {len(INDEX.records)} 条调用记录')
    print(f'  日志索引: {len(LOGS.by_file)} 个日志文件, {len(LOGS.records)} 条上报记录')
    print(f'  归档: 本次新入库 {n_new} 条，累计 {ARCHIVE.synced_calls} 条调用 / '
          f'{len(ARCHIVE.query_credits())} 条积分')
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
