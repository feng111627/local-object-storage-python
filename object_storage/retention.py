"""版本保留规则管理与淘汰判定。

- 规则持久化在 ``root/retention.json``，服务重启不丢；
- 规则可按精确 key 或前缀配置 ``max_versions`` / ``max_age_seconds``，随时查询调整；
- 淘汰只给版本打 ``expired`` 标记，数据文件由回收器（gc）物理删除；
- 以下版本在任何情况下都不会被淘汰：
  1. 当前对外可见的最新版本；
  2. 正被读取的版本（read pin）；
  3. 被显式冻结（frozen / legal hold）的版本。
- 已淘汰版本按版本号读取时返回 ``version_expired``，与本来不存在的
  ``no_such_version`` 是两个可区分的错误。
"""
from __future__ import annotations

import json
import threading
import time
import uuid

from .errors import InvalidRetentionRule, NoSuchRetentionRule
from .models import RetentionRule


class RetentionManager:
    """保留规则的增删改查与淘汰执行。"""

    def __init__(self, store):
        self.store = store
        self._lock = threading.RLock()
        self._path = self.store.root / "retention.json"

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _load(self) -> dict[str, dict]:
        if not self._path.exists():
            return {}
        data = json.loads(self._path.read_text(encoding="utf-8"))
        return {r["id"]: r for r in data.get("rules", [])}

    def _save(self, rules: dict[str, dict]) -> None:
        payload = {"rules": sorted(rules.values(), key=lambda r: r["id"])}
        self.store._atomic_write(self._path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))

    @staticmethod
    def _to_model(r: dict) -> RetentionRule:
        return RetentionRule(
            id=r["id"],
            scope=r["scope"],
            match=r["match"],
            max_versions=r.get("max_versions"),
            max_age_seconds=r.get("max_age_seconds"),
            created_at=r.get("created_at", 0.0),
            updated_at=r.get("updated_at", 0.0),
        )

    # ------------------------------------------------------------------
    # 规则管理
    # ------------------------------------------------------------------
    def put_rule(self, scope: str, match: str,
                 *, rule_id: str | None = None,
                 max_versions: int | None = None,
                 max_age_seconds: float | None = None) -> RetentionRule:
        """创建或更新一条规则（按 rule_id upsert；不传 id 则自动生成）。"""
        if scope not in ("key", "prefix"):
            raise InvalidRetentionRule(
                f"scope 只能是 key 或 prefix，收到 {scope!r}", detail={"scope": scope}
            )
        if not match:
            raise InvalidRetentionRule("match 不能为空", detail={"field": "match"})
        if max_versions is None and max_age_seconds is None:
            raise InvalidRetentionRule(
                "max_versions 与 max_age_seconds 至少要配置一个",
                detail={"fields": ["max_versions", "max_age_seconds"]},
            )
        if max_versions is not None and (not isinstance(max_versions, int) or max_versions < 1):
            raise InvalidRetentionRule(
                "max_versions 必须是 >=1 的整数", detail={"max_versions": max_versions}
            )
        if max_age_seconds is not None and max_age_seconds <= 0:
            raise InvalidRetentionRule(
                "max_age_seconds 必须是正数", detail={"max_age_seconds": max_age_seconds}
            )
        now = time.time()
        with self._lock:
            rules = self._load()
            rid = rule_id or uuid.uuid4().hex
            existing = rules.get(rid)
            created = existing.get("created_at", now) if existing else now
            rules[rid] = {
                "id": rid,
                "scope": scope,
                "match": match,
                "max_versions": max_versions,
                "max_age_seconds": max_age_seconds,
                "created_at": created,
                "updated_at": now,
            }
            self._save(rules)
            return self._to_model(rules[rid])

    def get_rule(self, rule_id: str) -> RetentionRule:
        with self._lock:
            rules = self._load()
            if rule_id not in rules:
                raise NoSuchRetentionRule(f"保留规则 {rule_id} 不存在", detail={"rule_id": rule_id})
            return self._to_model(rules[rule_id])

    def delete_rule(self, rule_id: str) -> None:
        with self._lock:
            rules = self._load()
            if rule_id not in rules:
                raise NoSuchRetentionRule(f"保留规则 {rule_id} 不存在", detail={"rule_id": rule_id})
            del rules[rule_id]
            self._save(rules)

    def list_rules(self) -> list[RetentionRule]:
        with self._lock:
            return [self._to_model(r) for r in self._load().values()]

    def rule_for_key(self, key: str) -> RetentionRule | None:
        """匹配优先级：精确 key 规则 > 最长前缀规则 > 无规则。"""
        best: dict | None = None
        with self._lock:
            for r in self._load().values():
                if r["scope"] == "key":
                    if r["match"] == key:
                        return self._to_model(r)
                elif key.startswith(r["match"]):
                    if best is None or len(r["match"]) > len(best["match"]):
                        best = r
        return self._to_model(best) if best else None

    # ------------------------------------------------------------------
    # 淘汰执行
    # ------------------------------------------------------------------
    def apply_once(self, *, now: float | None = None) -> dict:
        """对所有 key 应用一轮保留规则，返回淘汰统计。

        只做标记（expired=True），数据文件交给 GC 回收；
        被 pin 的版本即使超期也只标记、由 GC 等读完再删。
        """
        now = time.time() if now is None else now
        stats = {"keys_scanned": 0, "versions_expired": 0, "expired_bytes": 0, "rules_applied": 0}
        for key in self.store.iter_object_keys():
            stats["keys_scanned"] += 1
            rule = self.rule_for_key(key)
            if rule is None:
                continue
            stats["rules_applied"] += 1
            n, b = self._apply_to_key(key, rule, now)
            stats["versions_expired"] += n
            stats["expired_bytes"] += b
        return stats

    def _apply_to_key(self, key: str, rule: RetentionRule, now: float) -> tuple[int, int]:
        expired_count = 0
        expired_bytes = 0
        with self.store._key_locked(key):
            meta = self.store._load_meta(key)
            versions = meta["versions"]
            # 当前对外可见版本（最新的、未删/未淘汰/非标记/未冻结与否都不影响其可见保护）
            current_id = None
            for v in reversed(versions):
                if v.get("delete_marker"):
                    break
                if not v.get("deleted") and not v.get("expired"):
                    current_id = v["version_id"]
                    break

            # 仅对真正的数据版本做保留计数
            data_versions = [v for v in versions if not v.get("delete_marker")]

            # 1) 年龄淘汰：除当前版本与冻结版本外，超过 max_age_seconds 的一律淘汰。
            if rule.max_age_seconds is not None:
                cutoff = now - rule.max_age_seconds
                for v in data_versions:
                    if v["version_id"] == current_id or v.get("frozen"):
                        continue
                    if v.get("expired") or v.get("deleted"):
                        continue
                    if v["created_at"] <= cutoff:
                        v["expired"] = True
                        expired_count += 1
                        expired_bytes += v["size"]

            # 2) 数量保留：统计仍存活的数据版本，超出 max_versions 的最旧版本淘汰。
            if rule.max_versions is not None:
                live = [
                    v for v in data_versions
                    if not v.get("deleted") and not v.get("expired")
                ]
                # 当前版本与冻结版本永远占用名额且永不淘汰。
                if len(live) > rule.max_versions:
                    # 按创建时间从旧到新淘汰；保护集合之外、且非当前版本的优先。
                    protected = {current_id}
                    candidates = [v for v in live if v["version_id"] not in protected and not v.get("frozen")]
                    candidates.sort(key=lambda v: v["created_at"])
                    overflow = len(live) - rule.max_versions
                    for v in candidates[:overflow]:
                        v["expired"] = True
                        expired_count += 1
                        expired_bytes += v["size"]

            if expired_count:
                self.store._save_meta(key, meta)
        return expired_count, expired_bytes
