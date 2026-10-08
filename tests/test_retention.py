"""保留规则与版本淘汰测试：规则增删查、持久化、淘汰边界与保护语义。"""
from __future__ import annotations

import logging
import time

import pytest

from object_storage import (
    GarbageCollector,
    LocalObjectStore,
    RetentionStore,
)
from object_storage.errors import (
    NoSuchVersion,
    RetentionRuleInvalid,
    VersionAlreadyDeleted,
    VersionExpired,
    VersionOnHold,
)

logger = logging.getLogger("object_storage.tests.retention")


class FakeClock:
    """可控时钟：retention/gc 用 fake.now，store 的 created_at 用真实时间。"""

    def __init__(self, start: float | None = None):
        # 与 store 内部真实时间轴对齐，之后由我们控制推进。
        self.t = time.time() if start is None else start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture()
def retention(tmp_path):
    return RetentionStore(tmp_path / "rules")


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def gc(store, retention, clock):
    return GarbageCollector(store, retention=retention, upload_ttl=9999.0, clock=clock)


def _versions(store, key):
    return store.list_versions(key)  # 最新在前


class TestRuleManagement:
    def test_set_get_match_priority(self, retention):
        """精确 key 规则优先于前缀；最长前缀优先于短前缀。"""
        logger.info("业务覆盖：规则匹配优先级 key > 长前缀 > 短前缀")
        retention.set_rule("prefix", "logs/", max_versions=5)
        retention.set_rule("prefix", "logs/app/", max_versions=2)
        retention.set_rule("key", "logs/app/important.log", max_versions=1)
        assert retention.rule_for("logs/app/important.log").max_versions == 1
        assert retention.rule_for("logs/app/x.log").max_versions == 2
        assert retention.rule_for("logs/other.log").max_versions == 5
        assert retention.rule_for("unrelated") is None

    def test_upsert_replaces_existing(self, retention):
        logger.info("业务覆盖：同 scope+pattern 重复设置为覆盖更新")
        retention.set_rule("key", "k", max_versions=10)
        retention.set_rule("key", "k", max_versions=3, max_age_seconds=60)
        rules = retention.list_rules()
        assert len(rules) == 1
        assert rules[0].max_versions == 3 and rules[0].max_age_seconds == 60

    def test_delete_rule(self, retention):
        logger.info("业务覆盖：删除规则")
        retention.set_rule("prefix", "p/", max_versions=1)
        assert retention.delete_rule("prefix", "p/") is True
        assert retention.delete_rule("prefix", "p/") is False
        assert retention.rule_for("p/x") is None

    def test_invalid_rules_rejected(self, retention):
        logger.info("业务覆盖：非法规则参数明确拒绝")
        with pytest.raises(RetentionRuleInvalid):
            retention.set_rule("glob", "k")
        with pytest.raises(RetentionRuleInvalid):
            retention.set_rule("key", "", max_versions=1)
        with pytest.raises(RetentionRuleInvalid):
            retention.set_rule("key", "k", max_versions=0)
        with pytest.raises(RetentionRuleInvalid):
            retention.set_rule("key", "k", max_age_seconds=0)

    def test_rules_survive_restart(self, tmp_path):
        """规则写盘，重新打开存储后仍然生效。"""
        logger.info("业务覆盖：保留规则重启不丢")
        path = tmp_path / "rules"
        r1 = RetentionStore(path)
        r1.set_rule("prefix", "v/", max_versions=2, max_age_seconds=100)
        r2 = RetentionStore(path)
        rule = r2.rule_for("v/x")
        assert rule is not None and rule.max_versions == 2 and rule.max_age_seconds == 100


class TestExpiry:
    def test_max_versions_expires_old(self, store, gc, retention):
        """max_versions=2：3 个版本后最旧版本被淘汰，当前可见版本始终保留。"""
        logger.info("业务覆盖：按版本数淘汰旧版本、最新版本豁免")
        retention.set_rule("key", "k", max_versions=2)
        v1 = store.put_object("k", b"v1")
        time.sleep(0.002)
        v2 = store.put_object("k", b"v2")
        time.sleep(0.002)
        v3 = store.put_object("k", b"v3")
        stats = gc.collect_once()
        assert stats["expired_versions"] == 1
        # 最新版本仍可读
        info, data = store.get_object("k")
        assert info.version_id == v3.version_id and data == b"v3"
        # 被淘汰版本按版本号读 → version_expired（区别于不存在）
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)
        with pytest.raises(NoSuchVersion):
            store.get_object("k", "never-existed")
        # 清单里仍然能看到该版本，且标了 expired
        listing = {v.version_id: v for v in _versions(store, "k")}
        assert listing[v1.version_id].expired is True

    def test_max_age_expires_old(self, store, gc, retention, clock):
        """超龄版本被淘汰；未超龄的保留。最新版本即使超龄也不淘汰。"""
        logger.info("业务覆盖：按保留时长淘汰")
        retention.set_rule("key", "k", max_age_seconds=100)
        old = store.put_object("k", b"old")
        newest = store.put_object("k", b"new")
        clock.advance(50)
        assert gc.collect_once()["expired_versions"] == 0
        clock.advance(60)  # now 110 > 100
        stats = gc.collect_once()
        assert stats["expired_versions"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("k", old.version_id)
        # 最新版本豁免：仍可读
        assert store.get_object("k")[1] == b"new"

    def test_visible_version_protected_when_all_old(self, store, gc, retention, clock):
        """所有版本都超龄时，当前可见版本也绝不被淘汰（更旧的版本照常淘汰）。"""
        logger.info("业务覆盖：当前可见版本任何情况下保留")
        retention.set_rule("key", "k", max_age_seconds=10)
        old = store.put_object("k", b"a")
        clock.advance(0.002)
        only = store.put_object("k", b"b")
        clock.advance(1000)
        assert gc.collect_once()["expired_versions"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("k", old.version_id)
        assert store.get_object("k")[0].version_id == only.version_id

    def test_hold_version_never_expired_or_deleted(self, store, gc, retention):
        """显式冻结的版本既不能被规则淘汰，也不能被删除。"""
        logger.info("业务覆盖：冻结版本豁免淘汰与删除")
        retention.set_rule("key", "k", max_versions=1)
        v1 = store.put_object("k", b"first")
        time.sleep(0.002)
        store.put_object("k", b"second")
        store.set_version_hold("k", v1.version_id, True)
        assert gc.collect_once()["expired_versions"] == 0
        assert store.get_object("k", v1.version_id)[1] == b"first"
        with pytest.raises(VersionOnHold):
            store.delete_object("k", v1.version_id)
        # 解冻后规则即可将其淘汰
        store.set_version_hold("k", v1.version_id, False)
        assert gc.collect_once()["expired_versions"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("k", v1.version_id)

    def test_prefix_rule_applies_to_matching_keys(self, store, gc, retention):
        logger.info("业务覆盖：前缀规则作用于所有匹配 key，不匹配的不受影响")
        retention.set_rule("prefix", "tmp/", max_versions=1)
        a1 = store.put_object("tmp/a", b"a1")
        time.sleep(0.002)
        store.put_object("tmp/a", b"a2")
        b1 = store.put_object("keep/b", b"b1")
        time.sleep(0.002)
        store.put_object("keep/b", b"b2")
        stats = gc.collect_once()
        assert stats["expired_versions"] == 1
        with pytest.raises(VersionExpired):
            store.get_object("tmp/a", a1.version_id)
        assert store.get_object("keep/b", b1.version_id)[1] == b"b1"

    def test_expired_vs_deleted_distinct_errors(self, store, gc, retention):
        """淘汰回收后：expired -> version_expired；显式删除 -> version_already_deleted。"""
        logger.info("业务覆盖：淘汰与显式删除错误码严格区分")
        retention.set_rule("key", "k", max_versions=1)
        exp = store.put_object("k", b"exp")
        time.sleep(0.002)
        other_key_ver = store.put_object("other", b"x")
        store.delete_object("other", other_key_ver.version_id)
        store.put_object("k", b"latest")
        gc.collect_once()  # 两者数据文件都已回收
        with pytest.raises(VersionExpired):
            store.get_object("k", exp.version_id)
        with pytest.raises(VersionAlreadyDeleted):
            store.get_object("other", other_key_ver.version_id)

    def test_expiry_idempotent_across_rounds(self, store, gc, retention):
        logger.info("业务覆盖：多轮淘汰不重复计数、不报错")
        retention.set_rule("key", "k", max_versions=1)
        store.put_object("k", b"a")
        time.sleep(0.002)
        store.put_object("k", b"b")
        assert gc.collect_once()["expired_versions"] == 1
        assert gc.collect_once()["expired_versions"] == 0
        assert gc.collect_once()["reclaimed_version_files"] == 0

    def test_delete_marker_blocks_resurrection(self, store, gc, retention):
        """整 key 删除后，保留规则不会把旧版本重新变成可见。"""
        logger.info("业务覆盖：删除标记下淘汰不复活旧版本")
        retention.set_rule("key", "k", max_versions=1)
        v1 = store.put_object("k", b"a")
        time.sleep(0.002)
        v2 = store.put_object("k", b"b")
        store.delete_object("k")  # delete marker
        gc.collect_once()
        from object_storage.errors import NoSuchKey
        with pytest.raises(NoSuchKey):
            store.get_object("k")
