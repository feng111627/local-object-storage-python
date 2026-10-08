"""版本保留规则的存储与匹配。

规则持久化在 ``<root>/retention.json``，服务重启后仍然生效；所有读写都在
存储锁外自带的锁保护下完成（规则文件独立，不参与对象元数据事务）。
匹配优先级：精确 key 规则 > 最长前缀规则 > 无规则。
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from .errors import RetentionRuleInvalid
from .models import RetentionRule


class RetentionStore:
    def __init__(self, root: str | Path):
        self.path = Path(root) / "retention.json"
        self._lock = threading.RLock()
        self._rules: list[RetentionRule] = []
        self._load()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        self._rules = [
            RetentionRule(
                scope=r["scope"],
                pattern=r["pattern"],
                max_versions=r.get("max_versions"),
                max_age_seconds=r.get("max_age_seconds"),
                updated_at=r.get("updated_at", 0.0),
            )
            for r in raw.get("rules", [])
        ]

    def _persist_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "rules": [
                {
                    "scope": r.scope,
                    "pattern": r.pattern,
                    "max_versions": r.max_versions,
                    "max_age_seconds": r.max_age_seconds,
                    "updated_at": r.updated_at,
                }
                for r in self._rules
            ]
        }
        tmp = self.path.with_name(".retention.json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    # ------------------------------------------------------------------
    # 查询与调整
    # ------------------------------------------------------------------
    def list_rules(self) -> list[RetentionRule]:
        with self._lock:
            return list(self._rules)

    def set_rule(
        self,
        scope: str,
        pattern: str,
        *,
        max_versions: int | None = None,
        max_age_seconds: float | None = None,
    ) -> RetentionRule:
        if scope not in ("key", "prefix"):
            raise RetentionRuleInvalid(
                f"scope 只能是 key 或 prefix，收到 {scope!r}", detail={"scope": scope}
            )
        if not pattern:
            raise RetentionRuleInvalid("pattern 不能为空", detail={"scope": scope})
        if max_versions is not None and max_versions < 1:
            raise RetentionRuleInvalid(
                "max_versions 必须 >= 1", detail={"max_versions": max_versions}
            )
        if max_age_seconds is not None and max_age_seconds <= 0:
            raise RetentionRuleInvalid(
                "max_age_seconds 必须 > 0", detail={"max_age_seconds": max_age_seconds}
            )
        rule = RetentionRule(
            scope=scope,  # type: ignore[arg-type]
            pattern=pattern,
            max_versions=max_versions,
            max_age_seconds=max_age_seconds,
            updated_at=time.time(),
        )
        with self._lock:
            self._rules = [r for r in self._rules if not (r.scope == scope and r.pattern == pattern)]
            self._rules.append(rule)
            self._persist_locked()
        return rule

    def delete_rule(self, scope: str, pattern: str) -> bool:
        """删除一条规则，返回是否真的删到了。"""
        with self._lock:
            remaining = [r for r in self._rules if not (r.scope == scope and r.pattern == pattern)]
            removed = len(remaining) != len(self._rules)
            if removed:
                self._rules = remaining
                self._persist_locked()
            return removed

    def rule_for(self, key: str) -> RetentionRule | None:
        """返回作用于该 key 的最具体规则：精确匹配优先，否则取最长前缀。"""
        with self._lock:
            best: RetentionRule | None = None
            for r in self._rules:
                if r.scope == "key":
                    if r.pattern == key:
                        return r
                elif key.startswith(r.pattern):
                    if best is None or len(r.pattern) > len(best.pattern):
                        best = r
            return best
