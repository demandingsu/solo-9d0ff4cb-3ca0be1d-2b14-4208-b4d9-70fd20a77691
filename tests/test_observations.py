"""observations 批次入库口径单元测试（仅依赖标准库）：
python3 -m unittest discover -s tests -v
"""
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.evaluator import FormatError
from app.observations import (BatchConflict, ObservationStore, parse_events)


def ev(oid, rev, station="站点A", hour="2026-11-15T08", value=1.5, op="upsert"):
    e = {"observation_id": oid, "revision": rev, "op": op}
    if op == "upsert":
        e.update(station=station, hour=hour, value=value)
    return e


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = ObservationStore(os.path.join(self.tmp.name, "t.db"))

    def apply(self, events, name="t"):
        return self.store.apply_batch(name, parse_events(events))

    def summary(self):
        return {(r["station"], r["hour"]): (r["count"], r["total"])
                for r in self.store.summary()}


class TestParse(unittest.TestCase):
    def test_array_and_object_form(self):
        a = parse_events([ev("a", 1)])
        b = parse_events({"events": [ev("a", 1)]})
        self.assertEqual(a, b)
        self.assertEqual(a[0].observation_id, "a")
        self.assertEqual(a[0].value, 1.5)

    def test_revision_forms(self):
        self.assertEqual(parse_events([ev("a", 3)])[0].revision, 3)
        self.assertEqual(parse_events([ev("a", 3.0)])[0].revision, 3)
        for bad in (True, -1, 2.5, "3", None):
            with self.assertRaises(FormatError, msg=f"revision={bad!r}"):
                parse_events([ev("a", bad)])

    def test_bad_events(self):
        with self.assertRaises(FormatError):  # 空批次
            parse_events([])
        with self.assertRaises(FormatError):  # 缺 events 键
            parse_events({"foo": 1})
        with self.assertRaises(FormatError):  # 非对象事件
            parse_events(["x"])
        with self.assertRaises(FormatError):  # 空观测ID
            parse_events([ev("  ", 1)])
        with self.assertRaises(FormatError):  # 非法操作
            parse_events([ev("a", 1, op="delete")])
        with self.assertRaises(FormatError):  # 缺数值
            parse_events([{"observation_id": "a", "revision": 1, "op": "upsert",
                           "station": "s", "hour": "h"}])
        with self.assertRaises(FormatError):  # 非有限数值
            parse_events([ev("a", 1, value=float("nan"))])
        with self.assertRaises(FormatError):  # 布尔数值
            parse_events([ev("a", 1, value=True)])
        with self.assertRaises(FormatError):  # 空站点
            parse_events([ev("a", 1, station=" ")])

    def test_retract_ignores_payload_fields(self):
        (e,) = parse_events([{"observation_id": "a", "revision": 2, "op": "retract"}])
        self.assertIsNone(e.station)
        self.assertIsNone(e.value)


class TestApply(StoreCase):
    def test_basic_aggregation(self):
        self.apply([ev("a", 1, value=2.0), ev("b", 1, value=3.0),
                    ev("c", 1, station="站点B", value=5.0)])
        self.assertEqual(self.summary(), {
            ("站点A", "2026-11-15T08"): (2, 5.0),
            ("站点B", "2026-11-15T08"): (1, 5.0),
        })

    def test_higher_revision_correction_replaces_value(self):
        self.apply([ev("a", 1, value=2.0)])
        self.apply([ev("a", 2, value=7.0)])
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 7.0)})

    def test_correction_moves_station_and_hour(self):
        """更正站点/小时后，旧 (站点, 小时) 汇总须扣除旧值。"""
        self.apply([ev("a", 1, station="站点A", hour="2026-11-15T08", value=2.0),
                    ev("b", 1, station="站点A", hour="2026-11-15T08", value=3.0)])
        self.apply([ev("a", 2, station="站点B", hour="2026-11-15T09", value=10.0)])
        self.assertEqual(self.summary(), {
            ("站点A", "2026-11-15T08"): (1, 3.0),   # 旧值 2.0 已扣除
            ("站点B", "2026-11-15T09"): (1, 10.0),
        })

    def test_retract_deducts_and_blocks_older_revision(self):
        self.apply([ev("a", 3, value=4.0), ev("b", 1, value=1.0)])
        _, stats = self.apply([ev("a", 4, op="retract")])
        self.assertEqual(stats["retracted"], 1)
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 1.0)})
        # 撤回是墓碑：更低修订不得复活该观测
        _, stats = self.apply([ev("a", 2, value=99.0)])
        self.assertEqual(stats["stale"], 1)
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 1.0)})
        # 更高修订可以重新写入
        self.apply([ev("a", 5, value=8.0)])
        self.assertEqual(self.summary(),
                         {("站点A", "2026-11-15T08"): (2, 9.0)})

    def test_retract_unknown_observation_is_tombstone(self):
        _, stats = self.apply([ev("ghost", 5, op="retract")])
        self.assertEqual(stats["retracted"], 1)
        self.assertEqual(self.summary(), {})
        _, stats = self.apply([ev("ghost", 3, value=1.0)])  # 迟到的旧修订
        self.assertEqual(stats["stale"], 1)
        self.assertEqual(self.summary(), {})

    def test_stale_revision_never_rolls_back(self):
        self.apply([ev("a", 5, value=10.0)])
        _, stats = self.apply([ev("a", 4, value=999.0),
                               ev("a", 1, station="站点B", value=1.0)])
        self.assertEqual(stats["stale"], 2)
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 10.0)})

    def test_same_revision_same_content_deduped(self):
        self.apply([ev("a", 1, value=2.0)])
        _, stats = self.apply([ev("a", 1, value=2.0), ev("a", 1, value=2.0)])
        self.assertEqual(stats["deduped"], 2)
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 2.0)})

    def test_int_float_value_equivalence_dedupes(self):
        self.apply([ev("a", 1, value=2)])
        _, stats = self.apply([ev("a", 1, value=2.0)])
        self.assertEqual(stats["deduped"], 1)

    def test_same_revision_conflict_rejects_whole_batch(self):
        self.apply([ev("ok", 1, value=1.0)])
        with self.assertRaises(BatchConflict) as ctx:
            self.apply([ev("new", 1, value=5.0),      # 本可生效，但整批须拒绝
                        ev("ok", 1, value=999.0)])    # 同修订不同内容
        self.assertIn("ok", str(ctx.exception))       # 指出冲突观测ID
        # 整批回滚：新观测未入库，旧汇总原样保留
        self.assertEqual(self.summary(), {("站点A", "2026-11-15T08"): (1, 1.0)})

    def test_conflict_within_single_batch(self):
        with self.assertRaises(BatchConflict) as ctx:
            self.apply([ev("x", 1, value=1.0), ev("x", 1, value=2.0)])
        self.assertIn("x", str(ctx.exception))
        self.assertEqual(self.summary(), {})

    def test_conflict_lists_multiple_ids(self):
        self.apply([ev("a", 1), ev("b", 1)])
        with self.assertRaises(BatchConflict) as ctx:
            self.apply([ev("a", 1, value=9.0), ev("b", 1, value=9.0)])
        msg = str(ctx.exception)
        self.assertIn("a", msg)
        self.assertIn("b", msg)

    def test_within_batch_revision_order_irrelevant(self):
        for events in ([ev("a", 1, value=1.0), ev("a", 3, value=3.0)],
                       [ev("a", 3, value=3.0), ev("a", 1, value=1.0)]):
            store = ObservationStore(os.path.join(self.tmp.name, "o.db"))
            store.apply_batch("t", parse_events(events))
            cur = store.summary()
            self.assertEqual(cur, [{"station": "站点A", "hour": "2026-11-15T08",
                                    "count": 1, "total": 3.0}])
            os.remove(os.path.join(self.tmp.name, "o.db"))

    def test_failed_batch_recorded(self):
        with self.assertRaises(BatchConflict):
            self.apply([ev("a", 1), ev("a", 1, value=2.0)], name="坏批次")
        self.store.record_failed("坏批次", "同修订号内容冲突")
        batches = self.store.batches()
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["state"], "failed")
        self.assertIn("冲突", batches[0]["error"])

    def test_batch_stats_recorded(self):
        self.apply([ev("a", 1), ev("b", 1)])
        _, stats = self.apply([ev("a", 1), ev("b", 2, value=9.0),
                               ev("c", 1, op="retract")])
        self.assertEqual(stats, {"received": 3, "applied": 1, "retracted": 1,
                                 "deduped": 1, "stale": 0})
        hist = self.store.batches()
        self.assertEqual(hist[0]["state"], "applied")
        self.assertEqual(hist[0]["stats"]["applied"], 1)


class TestConcurrency(StoreCase):
    def test_concurrent_batches_lose_nothing(self):
        """两批同时上传：彼此的有效修订都不能丢，汇总为两批之和。"""
        batch1 = [ev(f"t1-{i}", 1, value=1.0) for i in range(30)]
        batch2 = [ev(f"t2-{i}", 1, value=2.0) for i in range(30)]
        # 两批含同一观测的相邻修订，检验串行化后最高修订生效
        batch1.append(ev("shared", 1, value=100.0))
        batch2.append(ev("shared", 2, value=200.0))

        errors = []

        def run(events):
            try:
                self.apply(events)
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=run, args=(b,)) for b in (batch1, batch2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        agg = self.summary()
        self.assertEqual(agg[("站点A", "2026-11-15T08")],
                         (61, 30 * 1.0 + 30 * 2.0 + 200.0))
        self.assertEqual(len(self.store.batches()), 2)


if __name__ == "__main__":
    unittest.main()
