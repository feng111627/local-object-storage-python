"""版本保留规则与淘汰语义测试。

覆盖：规则增删改查与重启持久化、按 key/前缀匹配（最长前缀优先）、
按数量/按年龄淘汰、当前可见版本/正在读取版本/冻结版本三类保护、
淘汰版本与「本来不存在」的错误可区分、规则放开后冻结版本仍存活。
"""
from __future__ import annotations

import logging
import threading

import pytest

from object_storage.errors import (
    InvalidRetentionRule,
    NoSuchKey,
    NoSuchRetentionRule,
    NoSuchVersion,
    VersionExpired,
    VersionProtected,
)
from object_storage.gc import GarbageCollector
from object_storage.retention import RetentionManager

logger = logging.getLogger("object_storage.tests.retention")


@pytest.fixture()
def retention(store):
    return RetentionManager(store)


class TestRuleManagement:
    def test_put_get_list_delete(self, retention):
        logger.info("业务覆盖：保留规则的增删改查")
        r = retention.put_rule("key", "a", max_versions=3)
        assert retention.get_rule(r.id).match == "a"
        assert [x.id for x in retention.list_rules()] == [r.id]
        # 更新（upsert）
        retention.put_rule("key", "a", rule_id=r.id, max_versions=5)
        assert retention.get_rule(r.id).max_versions == 5
        retention.delete_rule(r.id)
        assert retention.list_rules() == []
        with pytest.raises(NoSuchRetentionRule):
            retention.get_rule(r.id)

    def test_invalid_rules_rejected(self, retention):
        logger.info("业务覆盖：非法保留规则被明确拒绝")
        with pytest.raises(InvalidRetentionRule):
            retention.put_rule("badscope", "a", max_versions=1)
        with pytest.raises(InvalidRetentionRule):
            retention.put_rule("key", "", max_versions=1)
        with pytest.raises(InvalidRetentionRule):
            retention.put_rule("key", "a")  # 两个维度都没配
        with pytest.raises(InvalidRetentionRule):
            retention.put_rule("key", "a", max_versions=0)
        with pytest.raises(InvalidRetentionRule):
            retention.put_rule("key", "a", max_age_seconds=-1)

    def test_key_rule_wins_over_prefix_and_longest_prefix(self, retention):
        logger.info("业务覆盖：精确 key 优先，前缀取最长匹配")
        exact = retention.put_rule("key", "logs/a", max_versions=1)
        retention.put_rule("prefix", "logs/", max_versions=2)
        retention.put_rule("prefix", "logs/sub/", max_versions=9)
        got = retention.rule_for_key("logs/a")
        assert got.id == exact.id
        got = retention.rule_for_key("logs/sub/x")
        assert got.max_versions == 9
        got = retention.rule_for_key("logs/b")
        assert got.max_versions == 2
        assert retention.rule_for_key("other") is None

    def test_rules_persist_across_restart(self, store, tmp_path):
        logger.info("业务覆盖：规则重启不丢")
        rm1 = RetentionManager(store)
        r = rm1.put_rule("prefix", "logs/", max_versions=2, max_age_seconds=60)
        rm2 = RetentionManager(store)
        rules = rm2.list_rules()
        assert len(rules) == 1
        assert rules[0].id == r.id
        assert rules[0].max_versions == 2
        assert rules[0].max_age_seconds == 60


class TestExpiration:
    def test_max_versions_expires_oldest_keeps_current(self, store, retention):
        logger.info("业务覆盖：超量版本从最旧淘汰，当前可见版本永远保留")
        retention.put_rule("key", "k", max_versions=2)
        vs = [store.put_object("k", f"v{i}".encode()) for i in range(5)]
        stats = retention.apply_once()
        assert stats["versions_expired"] == 3
        # 最新两个可读；按版本号读旧版本得到 version_expired
        assert store.get_object("k")[1] == b"v4"
        with pytest.raises(VersionExpired):
            store.get_object("k", vs[0].version_id)
        # 淘汰可重复执行：幂等
        assert retention.apply_once()["versions_expired"] == 0

    def test_expired_distinct_from_missing_version(self, store, retention):
        logger.info("业务覆盖：已淘汰 vs 本来不存在，错误码可区分")
        v = store.put_object("k", b"x")
        retention.put_rule("key", "k", max_versions=1)
        store.put_object("k", b"y")
        retention.apply_once()
        with pytest.raises(VersionExpired):
            store.get_object("k", v.version_id)
        with pytest.raises(NoSuchVersion):
            store.get_object("k", "definitely-not-a-version")

    def test_max_age_expiration(self, store, retention):
        logger.info("业务覆盖：按年龄淘汰，当前版本与冻结版本豁免")
        old = store.put_object("k", b"old")
        current = store.put_object("k", b"new")
        retention.put_rule("key", "k", max_age_seconds=10)
        # 把旧版本时间往回拨 100 秒
        meta = store._load_meta("k")
        for vv in meta["versions"]:
            if vv["version_id"] == old.version_id:
                vv["created_at"] -= 100
        store._save_meta("k", meta)
        stats = retention.apply_once(now=current.created_at + 1)
        assert stats["versions_expired"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("k", old.version_id)
        assert store.get_object("k")[1] == b"new"  # 当前版本不因年龄被淘汰

    def test_prefix_rule_applies_to_matching_keys_only(self, store, retention):
        logger.info("业务覆盖：前缀规则只作用于匹配 key")
        retention.put_rule("prefix", "logs/", max_versions=1)
        logs_vs = [store.put_object("logs/a", b"x"), store.put_object("logs/a", b"y")]
        other = [store.put_object("etc/a", b"x"), store.put_object("etc/a", b"y")]
        retention.apply_once()
        with pytest.raises(VersionExpired):
            store.get_object("logs/a", logs_vs[0].version_id)
        # 无规则的 key 全部保留
        assert store.get_object("etc/a", other[0].version_id)[1] == b"x"

    def test_frozen_version_never_expired_and_delete_protected(self, store, retention):
        logger.info("业务覆盖：冻结版本不被淘汰，且拒绝显式删除")
        v1 = store.put_object("k", b"v1")
        v2 = store.put_object("k", b"v2")
        v3 = store.put_object("k", b"v3")
        store.freeze_version("k", v1.version_id)
        retention.put_rule("key", "k", max_versions=1)
        # max_versions=1：当前 v3 保留；冻结的 v1 占用保护名额，只有 v2 被淘汰
        stats = retention.apply_once()
        assert stats["versions_expired"] == 1
        assert store.get_object("k", v1.version_id)[1] == b"v1"
        with pytest.raises(VersionExpired):
            store.get_object("k", v2.version_id)
        with pytest.raises(VersionProtected):
            store.delete_object("k", v1.version_id)
        # 解冻后仍可读，再跑规则才会被数量淘汰
        store.unfreeze_version("k", v1.version_id)
        retention.apply_once()
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)

    def test_freeze_revives_expired_version_before_reclaim(self, store, retention):
        logger.info("业务覆盖：已淘汰但数据未回收的版本，冻结会撤销淘汰（legal hold 优先）")
        v1 = store.put_object("k", b"v1")
        v2 = store.put_object("k", b"v2")
        retention.put_rule("key", "k", max_versions=1)
        retention.apply_once()
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)
        # 尚未物理回收：冻结恢复该版本
        store.freeze_version("k", v1.version_id)
        assert store.get_object("k", v1.version_id)[1] == b"v1"
        # 再跑规则也不会淘汰冻结版本
        assert retention.apply_once()["versions_expired"] == 0
        assert store.get_object("k", v1.version_id)[1] == b"v1"

    def test_delete_marker_does_not_revive_and_not_counted(self, store, retention):
        logger.info("业务覆盖：删除标记阻断可见性，且不参与保留计数")
        v1 = store.put_object("k", b"v1")
        v2 = store.put_object("k", b"v2")
        store.delete_object("k")  # 整 key 删除标记
        retention.put_rule("key", "k", max_versions=1)
        # 删除标记在最前：没有「当前可见版本」，数据版本仍按数量规则评估
        retention.apply_once()
        with pytest.raises(NoSuchKey):
            store.get_object("k")
        # v1 被淘汰，v2 作为仅存存活数据版本保留（按版本号可读）
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)
        assert store.get_object("k", v2.version_id)[1] == b"v2"


class TestExpirationWithReclaim:
    def test_expired_data_reclaimed_by_gc_but_error_stays_distinct(self, store, retention):
        logger.info("业务覆盖：淘汰后 GC 回收数据，错误仍为 version_expired")
        v1 = store.put_object("k", b"old")
        store.put_object("k", b"new")
        retention.put_rule("key", "k", max_versions=1)
        retention.apply_once()
        stats = GarbageCollector(store).collect_once()
        assert stats["reclaimed_expired_files"] == 1
        assert stats["reclaimed_bytes"] == 3
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)

    def test_being_read_version_not_reclaimed(self, store, retention):
        logger.info("业务覆盖：正在读取的版本不会被回收，读完后下轮清掉")
        store.reclaim_wait_seconds = 0.3
        retention.put_rule("key", "k", max_versions=1)
        v1 = store.put_object("k", b"oldblock")
        store.put_object("k", b"newblock")
        retention.apply_once()
        entered = threading.Event()
        release = threading.Event()

        def reader():
            # 模拟真实读取：get_object 在 key 锁内登记 pin，随后释放 key 锁再读文件，
            # 因此这里只持有 pin、不持有 key 锁。
            store._pin("k", v1.version_id)
            entered.set()
            release.wait(timeout=5)
            store._unpin("k", v1.version_id)

        t = threading.Thread(target=reader)
        t.start()
        entered.wait(timeout=5)
        gc = GarbageCollector(store)
        first = gc.collect_once()  # v1 正在被 pin，等待超时后本轮跳过
        assert first["skipped_pinned"] >= 1
        assert first["reclaimed_expired_files"] == 0
        release.set()
        t.join()
        second = gc.collect_once()
        assert second["reclaimed_expired_files"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)
