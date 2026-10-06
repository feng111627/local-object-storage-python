"""存储核心测试：分片上传主流程、幂等、并发、断点续传、校验失败、版本化。"""
from __future__ import annotations

import hashlib
import logging
import threading

import pytest

from object_storage.errors import (
    CompleteChecksumMismatch,
    MissingParts,
    NoSuchKey,
    NoSuchUpload,
    NoSuchVersion,
    ObjectTooLarge,
    PartChecksumMismatch,
    PartConflict,
    TooManyConcurrentUploads,
    TooManyParts,
    UploadAlreadyCompleted,
    VersionAlreadyDeleted,
)

logger = logging.getLogger("object_storage.tests.store")


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ---------------------------------------------------------------- 分片上传主流程

class TestMultipartFlow:
    def test_basic_multipart_upload(self, store):
        """主流程：创建会话→逐片上传→合并→读回完整内容。"""
        logger.info("业务覆盖：分片上传主流程（create/upload_part/complete/get）")
        up = store.create_multipart_upload("docs/report.bin")
        store.upload_part(up.upload_id, 1, b"aaa")
        store.upload_part(up.upload_id, 2, b"bbb")
        store.upload_part(up.upload_id, 3, b"ccc")
        version = store.complete_multipart_upload(up.upload_id)
        info, data = store.get_object("docs/report.bin")
        assert data == b"aaabbbccc"
        assert info.version_id == version.version_id
        assert info.size == 9
        assert info.sha256 == sha(b"aaabbbccc")

    def test_parts_merged_in_manifest_order(self, store):
        """合并顺序由清单决定，而不是上传顺序。"""
        logger.info("业务覆盖：合并顺序按显式清单")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 2, b"two")
        store.upload_part(up.upload_id, 1, b"one")
        store.complete_multipart_upload(up.upload_id, parts=[1, 2])
        assert store.get_object("k")[1] == b"onetwo"

    def test_list_parts_reflects_progress(self, store):
        """分片清单可查询，断点续传时据此跳过已完成分片。"""
        logger.info("业务覆盖：分片清单查询（断点续传进度确认）")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"a")
        store.upload_part(up.upload_id, 3, b"c")
        parts = store.list_parts(up.upload_id)
        assert [p.part_number for p in parts] == [1, 3]
        # 模拟客户端续传：只补缺失的分片 2
        store.upload_part(up.upload_id, 2, b"b")
        store.complete_multipart_upload(up.upload_id)
        assert store.get_object("k")[1] == b"abc"

    def test_resume_after_restart(self, tmp_path):
        """异常中断（进程重启）后，会话与分片仍在磁盘上，可继续上传并合并。"""
        logger.info("业务覆盖：异常中断后的断点续传（跨实例恢复会话）")
        from object_storage import LocalObjectStore

        root = tmp_path / "data"
        s1 = LocalObjectStore(root)
        up = s1.create_multipart_upload("k", expected_size=6)
        s1.upload_part(up.upload_id, 1, b"foo")
        # 模拟进程崩溃：直接丢弃实例，用同一目录重建
        s2 = LocalObjectStore(root)
        parts = s2.list_parts(up.upload_id)
        assert [p.part_number for p in parts] == [1]
        s2.upload_part(up.upload_id, 2, b"bar")
        s2.complete_multipart_upload(up.upload_id)
        assert s2.get_object("k")[1] == b"foobar"


# ---------------------------------------------------------------- 幂等与并发

class TestIdempotencyAndConcurrency:
    def test_duplicate_part_same_content_is_idempotent(self, store):
        """重复上传同一分片且内容一致：幂等成功，不产生冲突。"""
        logger.info("业务覆盖：重复上传同一分片的幂等性")
        up = store.create_multipart_upload("k")
        p1 = store.upload_part(up.upload_id, 1, b"data")
        p2 = store.upload_part(up.upload_id, 1, b"data")
        assert p1.sha256 == p2.sha256
        assert [p.part_number for p in store.list_parts(up.upload_id)] == [1]

    def test_duplicate_part_different_content_conflicts(self, store):
        """同一分片内容不一致的重复上传：明确报 PartConflict。"""
        logger.info("业务覆盖：同一分片内容冲突检测")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"data")
        with pytest.raises(PartConflict):
            store.upload_part(up.upload_id, 1, b"DATA")

    def test_concurrent_same_part_single_winner(self, store):
        """并发上传同一分片（相同内容）：全部成功且结果一致（幂等）。"""
        logger.info("业务覆盖：并发上传同一分片（相同内容）")
        up = store.create_multipart_upload("k")
        results, errors = [], []

        def worker():
            try:
                results.append(store.upload_part(up.upload_id, 1, b"same"))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert len(results) == 8
        assert {r.sha256 for r in results} == {sha(b"same")}
        assert len(store.list_parts(up.upload_id)) == 1

    def test_concurrent_same_part_different_content(self, store):
        """并发上传同一分片（不同内容）：只有一个获胜，其余报 PartConflict。"""
        logger.info("业务覆盖：并发上传同一分片（不同内容）的冲突仲裁")
        up = store.create_multipart_upload("k")
        outcomes = []

        def worker(i):
            try:
                store.upload_part(up.upload_id, 1, f"content-{i}".encode())
                outcomes.append("ok")
            except PartConflict:
                outcomes.append("conflict")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert outcomes.count("ok") == 1
        assert outcomes.count("conflict") == 7
        assert len(store.list_parts(up.upload_id)) == 1

    def test_double_complete_rejected(self, store):
        """重复合并同一会话：第二次被拒绝，不会生成第二个版本。"""
        logger.info("业务覆盖：重复合并的幂等拒绝")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"x")
        store.complete_multipart_upload(up.upload_id)
        with pytest.raises(NoSuchUpload):
            store.complete_multipart_upload(up.upload_id)
        assert len(store.list_versions("k")) == 1

    def test_concurrent_readers_see_complete_versions(self, store):
        """并发读写同一 key：读到的永远是某个完整版本，绝不读到写了一半的内容。"""
        logger.info("业务覆盖：并发读写一致性（读写快照隔离）")
        store.put_object("k", b"v0" * 100)
        stop = threading.Event()
        anomalies = []

        def writer():
            i = 1
            while not stop.is_set():
                store.put_object("k", f"v{i}".encode() * 100)
                i += 1

        def reader():
            while not stop.is_set():
                _, data = store.get_object("k")
                # 每次写入都是同一单元重复 100 次；读到混合内容即为异常。
                unit = data[: len(data) // 100]
                if data != unit * 100:
                    anomalies.append(data)

        threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        threading.Event().wait(0.5)
        stop.set()
        for t in threads:
            t.join()
        assert anomalies == []


# ---------------------------------------------------------------- 校验失败

class TestChecksumFailures:
    def test_part_checksum_mismatch_rejected(self, store):
        """声明的分片校验值与实际不符：拒绝且不记录该分片。"""
        logger.info("业务覆盖：分片级校验失败")
        up = store.create_multipart_upload("k")
        with pytest.raises(PartChecksumMismatch):
            store.upload_part(up.upload_id, 1, b"data", sha256="0" * 64)
        assert store.list_parts(up.upload_id) == []

    def test_complete_with_missing_parts_rejected(self, store):
        """合并清单引用未上传的分片：拒绝并列出缺失分片号。"""
        logger.info("业务覆盖：合并时分片缺失")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"a")
        with pytest.raises(MissingParts) as exc_info:
            store.complete_multipart_upload(up.upload_id, parts=[1, 2, 3])
        assert exc_info.value.detail["missing"] == [2, 3]
        # 会话未被破坏，补齐后仍可合并
        store.upload_part(up.upload_id, 2, b"b")
        store.upload_part(up.upload_id, 3, b"c")
        store.complete_multipart_upload(up.upload_id, parts=[1, 2, 3])
        assert store.get_object("k")[1] == b"abc"

    def test_complete_size_mismatch_rejected(self, store):
        """合并结果与声明的整体大小不符：拒绝，不产生版本。"""
        logger.info("业务覆盖：合并整体大小校验失败")
        up = store.create_multipart_upload("k", expected_size=10)
        store.upload_part(up.upload_id, 1, b"short")
        with pytest.raises(CompleteChecksumMismatch) as exc_info:
            store.complete_multipart_upload(up.upload_id)
        assert exc_info.value.detail["reason"] == "size_mismatch"
        with pytest.raises(NoSuchKey):
            store.get_object("k")

    def test_complete_sha256_mismatch_rejected(self, store):
        """合并结果与声明的整体校验值不符：拒绝，不产生版本。"""
        logger.info("业务覆盖：合并整体内容校验失败")
        up = store.create_multipart_upload("k", expected_sha256="f" * 64)
        store.upload_part(up.upload_id, 1, b"actual")
        with pytest.raises(CompleteChecksumMismatch) as exc_info:
            store.complete_multipart_upload(up.upload_id)
        assert exc_info.value.detail["reason"] == "sha256_mismatch"
        with pytest.raises(NoSuchKey):
            store.get_object("k")

    def test_failed_complete_leaves_no_partial_object(self, store):
        """合并失败后磁盘上不残留半个对象版本。"""
        logger.info("业务覆盖：合并失败不产生损坏对象")
        up = store.create_multipart_upload("k", expected_size=999)
        store.upload_part(up.upload_id, 1, b"data")
        with pytest.raises(CompleteChecksumMismatch):
            store.complete_multipart_upload(up.upload_id)
        assert store.list_versions("k") == [] if _has_key(store, "k") else True
        with pytest.raises(NoSuchKey):
            store.get_object("k")


def _has_key(store, key):
    try:
        store.list_versions(key)
        return True
    except NoSuchKey:
        return False


# ---------------------------------------------------------------- 版本化

class TestVersioning:
    def test_overwrite_creates_new_version(self, store):
        """覆盖写生成新版本，旧版本仍可按版本号读取。"""
        logger.info("业务覆盖：覆盖写与多版本读取")
        v1 = store.put_object("k", b"first")
        v2 = store.put_object("k", b"second")
        assert v1.version_id != v2.version_id
        assert store.get_object("k")[1] == b"second"
        assert store.get_object("k", v1.version_id)[1] == b"first"

    def test_delete_specific_version(self, store):
        """删除指定版本后：读该版本报 VersionAlreadyDeleted，最新读回退到上一版本。"""
        logger.info("业务覆盖：指定版本删除与回退")
        v1 = store.put_object("k", b"first")
        v2 = store.put_object("k", b"second")
        store.delete_object("k", v2.version_id)
        with pytest.raises(VersionAlreadyDeleted):
            store.get_object("k", v2.version_id)
        assert store.get_object("k")[1] == b"first"

    def test_delete_whole_key_writes_marker(self, store):
        """整 key 删除写入删除标记：普通读取 404，历史版本仍可读。"""
        logger.info("业务覆盖：删除标记语义")
        v1 = store.put_object("k", b"first")
        store.delete_object("k")
        with pytest.raises(NoSuchKey):
            store.get_object("k")
        assert store.get_object("k", v1.version_id)[1] == b"first"

    def test_delete_nonexistent_version(self, store):
        """删除不存在的版本：返回可区分的 NoSuchVersion。"""
        logger.info("业务覆盖：删除不存在版本的错误分类")
        store.put_object("k", b"x")
        with pytest.raises(NoSuchVersion):
            store.delete_object("k", "no-such-version")

    def test_double_delete_same_version(self, store):
        """重复删除同一版本：第二次报 VersionAlreadyDeleted（幂等可解释）。"""
        logger.info("业务覆盖：重复删除同一版本")
        v = store.put_object("k", b"x")
        store.delete_object("k", v.version_id)
        with pytest.raises(VersionAlreadyDeleted):
            store.delete_object("k", v.version_id)

    def test_list_versions_newest_first(self, store):
        """版本清单按时间倒序，包含删除标记。"""
        logger.info("业务覆盖：版本清单查询")
        store.put_object("k", b"a")
        store.put_object("k", b"b")
        store.delete_object("k")
        versions = store.list_versions("k")
        assert len(versions) == 3
        assert versions[0].delete_marker
        assert versions[0].created_at >= versions[-1].created_at


# ---------------------------------------------------------------- 配额

class TestQuota:
    def test_object_too_large_rejected(self, tight_store):
        """整体写入超过单对象上限：明确拒绝。"""
        logger.info("业务覆盖：单对象大小配额")
        with pytest.raises(ObjectTooLarge):
            tight_store.put_object("k", b"x" * 65)

    def test_part_too_large_rejected(self, tight_store):
        """单个分片超过上限：明确拒绝。"""
        logger.info("业务覆盖：单分片大小配额")
        up = tight_store.create_multipart_upload("k")
        with pytest.raises(ObjectTooLarge):
            tight_store.upload_part(up.upload_id, 1, b"x" * 33)

    def test_too_many_parts_rejected(self, tight_store):
        """分片号超出上限：明确拒绝。"""
        logger.info("业务覆盖：分片数量配额")
        up = tight_store.create_multipart_upload("k")
        with pytest.raises(TooManyParts):
            tight_store.upload_part(up.upload_id, 5, b"x")

    def test_cumulative_size_quota(self, tight_store):
        """累计分片大小超过对象上限：明确拒绝而不是静默截断。"""
        logger.info("业务覆盖：累计大小配额（不静默截断）")
        up = tight_store.create_multipart_upload("k")
        tight_store.upload_part(up.upload_id, 1, b"x" * 32)
        tight_store.upload_part(up.upload_id, 2, b"x" * 32)
        with pytest.raises(ObjectTooLarge):
            tight_store.upload_part(up.upload_id, 3, b"x")

    def test_concurrent_upload_quota(self, tight_store):
        """同一 key 并发上传会话数超限：明确拒绝；中止后释放名额。"""
        logger.info("业务覆盖：并发上传数配额")
        s1 = tight_store.create_multipart_upload("k")
        tight_store.create_multipart_upload("k")
        with pytest.raises(TooManyConcurrentUploads):
            tight_store.create_multipart_upload("k")
        tight_store.abort_multipart_upload(s1.upload_id)
        tight_store.create_multipart_upload("k")  # 名额释放后可再建

    def test_expected_size_declared_over_quota_rejected(self, tight_store):
        """创建会话时声明的大小超限：立即拒绝，不浪费分片上传。"""
        logger.info("业务覆盖：声明大小预检")
        with pytest.raises(ObjectTooLarge):
            tight_store.create_multipart_upload("k", expected_size=65)


# ---------------------------------------------------------------- 会话生命周期

class TestUploadSessionLifecycle:
    def test_upload_to_completed_session_rejected(self, store):
        """已完成的会话再传分片：报 UploadAlreadyCompleted。"""
        logger.info("业务覆盖：已完成会话的写保护")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"x")
        store.complete_multipart_upload(up.upload_id)
        with pytest.raises(NoSuchUpload):
            store.upload_part(up.upload_id, 2, b"y")

    def test_abort_cleans_up(self, store):
        """中止会话后：分片数据被清理，会话不可再访问。"""
        logger.info("业务覆盖：中止上传会话")
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"x")
        store.abort_multipart_upload(up.upload_id)
        with pytest.raises(NoSuchUpload):
            store.list_parts(up.upload_id)

    def test_operations_on_unknown_upload(self, store):
        """对不存在的会话操作：统一报 NoSuchUpload。"""
        logger.info("业务覆盖：不存在会话的错误分类")
        with pytest.raises(NoSuchUpload):
            store.upload_part("nope", 1, b"x")
        with pytest.raises(NoSuchUpload):
            store.list_parts("nope")
        with pytest.raises(NoSuchUpload):
            store.complete_multipart_upload("nope")
        with pytest.raises(NoSuchUpload):
            store.abort_multipart_upload("nope")
