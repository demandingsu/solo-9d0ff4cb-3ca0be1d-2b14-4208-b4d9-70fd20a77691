"""站点观测批次入库：事件解析校验 + SQLite 事务化应用。

口径：
- 每个观测（observation_id）跨批次只采用**最高修订号**的记录；较低修订一律视为过期，
  不得回滚已有结果。
- 同一修订号重投且内容一致（操作、站点、小时、数值完全相同）→ 幂等去重，只计一次；
  内容不一致 → 整批拒绝（事务回滚，旧汇总原样保留），错误信息指出冲突的观测ID。
- 撤回（retract）或更正（upsert 新修订改变站点/小时/数值）时，先从原 (站点, 小时)
  汇总中**扣除旧值**，再计入新值；撤回后该观测成为墓碑，更低修订不得复活它，
  更高修订可以重新写入。
- 整批在一个 BEGIN IMMEDIATE 事务内应用：并发上传的批次串行生效，互不丢修订；
  汇总表只在提交时变化，查询永远只读到完整已发布的结果。
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from .evaluator import FormatError

OP_UPSERT = "upsert"    # 写入 / 更正（新修订）
OP_RETRACT = "retract"  # 撤回

SCHEMA = """
CREATE TABLE IF NOT EXISTS obs_current (
    observation_id TEXT PRIMARY KEY,
    revision       INTEGER NOT NULL,
    op             TEXT NOT NULL CHECK (op IN ('upsert', 'retract')),
    station        TEXT,
    hour           TEXT,
    value          REAL,
    batch_id       INTEGER NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS obs_aggregates (
    station TEXT NOT NULL,
    hour    TEXT NOT NULL,
    cnt     INTEGER NOT NULL,
    total   REAL NOT NULL,
    PRIMARY KEY (station, hour)
);
CREATE TABLE IF NOT EXISTS obs_batches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    state      TEXT NOT NULL CHECK (state IN ('applied', 'failed')),
    error      TEXT,
    stats      TEXT
);
"""

MAX_CONFLICTS_REPORTED = 10  # 错误信息中最多列出的冲突观测数


class BatchConflict(Exception):
    """同修订号内容冲突：整批拒绝，消息中含冲突观测ID。"""


@dataclass(frozen=True)
class ObsEvent:
    observation_id: str
    revision: int
    op: str
    station: Optional[str]   # retract 事件可为 None
    hour: Optional[str]
    value: Optional[float]


# ---------------------------------------------------------------------------
# 事件解析与校验
# ---------------------------------------------------------------------------

def _parse_revision(raw: Any, where: str) -> int:
    if isinstance(raw, bool):
        raise FormatError(f"{where}：修订号必须是非负整数，不能是布尔值")
    if isinstance(raw, int):
        rev = raw
    elif isinstance(raw, float) and raw.is_integer() and math.isfinite(raw):
        rev = int(raw)
    else:
        raise FormatError(f"{where}：修订号必须是非负整数：{raw!r}")
    if rev < 0:
        raise FormatError(f"{where}：修订号不能为负数：{rev}")
    return rev


def _parse_text(raw: Any, field: str, where: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise FormatError(f"{where}：{field} 必须是非空字符串：{raw!r}")
    return raw.strip()


def _parse_value(raw: Any, where: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise FormatError(f"{where}：数值必须是 JSON 数字：{raw!r}")
    v = float(raw)
    if not math.isfinite(v):
        raise FormatError(f"{where}：数值必须是有限数字：{raw!r}")
    return v


def parse_events(data: Any) -> list[ObsEvent]:
    """解析批次 JSON：接受事件数组或 {"events": [...]}。

    事件格式：{"observation_id", "revision", "station", "hour", "value", "op"}
    op 为 "upsert"（写入/更正）或 "retract"（撤回，站点/小时/数值可缺省）。
    """
    if isinstance(data, dict):
        if "events" not in data:
            raise FormatError("批次对象必须包含 events 数组")
        items = data["events"]
    else:
        items = data
    if not isinstance(items, list) or not items:
        raise FormatError("批次必须是非空的事件数组（或含非空 events 数组的对象）")

    events: list[ObsEvent] = []
    for i, item in enumerate(items, 1):
        where = f"第 {i} 条事件"
        if not isinstance(item, dict):
            raise FormatError(f"{where}：必须是 JSON 对象：{item!r}")
        oid = _parse_text(item.get("observation_id"), "观测ID(observation_id)", where)
        rev = _parse_revision(item.get("revision"), where)
        op = item.get("op")
        if op not in (OP_UPSERT, OP_RETRACT):
            raise FormatError(
                f"{where}（观测 {oid}）：操作 op 必须是 "
                f"'{OP_UPSERT}'（写入/更正）或 '{OP_RETRACT}'（撤回）：{op!r}")
        if op == OP_UPSERT:
            station = _parse_text(item.get("station"), "站点(station)", where)
            hour = _parse_text(item.get("hour"), "小时(hour)", where)
            if "value" not in item:
                raise FormatError(f"{where}（观测 {oid}）：缺少数值 value")
            value = _parse_value(item["value"], where)
        else:
            station = hour = None
            value = None
        events.append(ObsEvent(observation_id=oid, revision=rev, op=op,
                               station=station, hour=hour, value=value))
    return events


# ---------------------------------------------------------------------------
# 存储与批次应用
# ---------------------------------------------------------------------------

class ObservationStore:
    """观测有效状态 + (站点, 小时) 汇总，与快照库共用同一 SQLite 文件。"""

    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()
        conn = self._conn()
        try:
            conn.executescript(SCHEMA)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    # -- 汇总增减 -----------------------------------------------------------

    @staticmethod
    def _agg_add(conn: sqlite3.Connection, station: str, hour: str,
                 dcnt: int, dtotal: float) -> None:
        conn.execute(
            "INSERT INTO obs_aggregates(station, hour, cnt, total) VALUES (?,?,?,?) "
            "ON CONFLICT(station, hour) DO UPDATE SET cnt = cnt + ?, total = total + ?",
            (station, hour, dcnt, dtotal, dcnt, dtotal))
        # 记录数归零的 (站点, 小时) 直接移除，查询结果保持干净
        conn.execute(
            "DELETE FROM obs_aggregates WHERE station=? AND hour=? AND cnt=0",
            (station, hour))

    # -- 单事件应用（须在事务内调用） ----------------------------------------

    @staticmethod
    def _same_content(row: sqlite3.Row, ev: ObsEvent) -> bool:
        if row["op"] != ev.op:
            return False
        if ev.op == OP_RETRACT:
            return True  # 撤回只看操作本身
        return (row["station"] == ev.station and row["hour"] == ev.hour
                and row["value"] == ev.value)

    def _apply_one(self, conn: sqlite3.Connection, ev: ObsEvent, batch_id: int,
                   stats: dict, conflicts: list[str]) -> None:
        row = conn.execute(
            "SELECT * FROM obs_current WHERE observation_id=?",
            (ev.observation_id,)).fetchone()
        now = self._now()

        if row is not None and ev.revision < row["revision"]:
            stats["stale"] += 1  # 旧修订：不得回滚已有结果
            return
        if row is not None and ev.revision == row["revision"]:
            if self._same_content(row, ev):
                stats["deduped"] += 1  # 同修订重投且内容一致：只计一次
            else:
                conflicts.append(ev.observation_id)  # 同修订内容冲突：整批拒绝
            return

        # 新观测或更高修订：先扣除旧值，再计入新值
        if row is not None and row["op"] == OP_UPSERT:
            self._agg_add(conn, row["station"], row["hour"], -1, -row["value"])
        if ev.op == OP_UPSERT:
            self._agg_add(conn, ev.station, ev.hour, 1, ev.value)
            stats["applied"] += 1
        else:
            stats["retracted"] += 1
        conn.execute(
            "INSERT INTO obs_current(observation_id, revision, op, station, hour, value,"
            " batch_id, updated_at) VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(observation_id) DO UPDATE SET revision=excluded.revision,"
            " op=excluded.op, station=excluded.station, hour=excluded.hour,"
            " value=excluded.value, batch_id=excluded.batch_id,"
            " updated_at=excluded.updated_at",
            (ev.observation_id, ev.revision, ev.op, ev.station, ev.hour,
             ev.value, batch_id, now))

    # -- 批次入口 -----------------------------------------------------------

    def apply_batch(self, name: str, events: list[ObsEvent]) -> tuple[int, dict]:
        """整批事务化应用。冲突时抛 BatchConflict，汇总保持旧结果不变。"""
        stats = {"received": len(events), "applied": 0,
                 "retracted": 0, "deduped": 0, "stale": 0}
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                # 先占位取批次号，事件落库时记录归属
                cur = conn.execute(
                    "INSERT INTO obs_batches(name, created_at, state) VALUES (?,?, 'applied')",
                    (name or "(未命名批次)", self._now()))
                batch_id = int(cur.lastrowid)
                conflicts: list[str] = []
                for ev in events:
                    self._apply_one(conn, ev, batch_id, stats, conflicts)
                if conflicts:
                    uniq = list(dict.fromkeys(conflicts))
                    shown = "、".join(uniq[:MAX_CONFLICTS_REPORTED])
                    more = f" 等 {len(uniq)} 个" if len(uniq) > MAX_CONFLICTS_REPORTED else ""
                    raise BatchConflict(
                        f"同修订号内容冲突，整批已拒绝（旧结果保留）。冲突观测ID：{shown}{more}")
                conn.execute("UPDATE obs_batches SET stats=? WHERE id=?",
                             (json.dumps(stats, ensure_ascii=False), batch_id))
                conn.commit()
                return batch_id, stats
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def record_failed(self, name: str, error: str) -> int:
        """失败批次留痕（格式错误或冲突），不影响已发布汇总。"""
        with self._lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "INSERT INTO obs_batches(name, created_at, state, error)"
                    " VALUES (?,?, 'failed', ?)",
                    (name or "(未命名批次)", self._now(), error))
                conn.commit()
                return int(cur.lastrowid)
            finally:
                conn.close()

    # -- 查询（只读已提交结果） ----------------------------------------------

    def summary(self, station: Optional[str] = None) -> list[dict[str, Any]]:
        sql = "SELECT station, hour, cnt, total FROM obs_aggregates"
        args: tuple = ()
        if station:
            sql += " WHERE station = ?"
            args = (station,)
        sql += " ORDER BY station, hour"
        conn = self._conn()
        try:
            rows = conn.execute(sql, args).fetchall()
            return [{"station": r["station"], "hour": r["hour"],
                     "count": r["cnt"], "total": r["total"]} for r in rows]
        finally:
            conn.close()

    def batches(self, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT id, name, created_at, state, error, stats FROM obs_batches "
                "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            items = []
            for r in rows:
                items.append({"id": r["id"], "name": r["name"],
                              "created_at": r["created_at"], "state": r["state"],
                              "error": r["error"],
                              "stats": json.loads(r["stats"]) if r["stats"] else None})
            return items
        finally:
            conn.close()
