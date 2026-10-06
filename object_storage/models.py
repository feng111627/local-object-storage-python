"""数据模型：版本、分片、上传会话等。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class VersionRecord:
    """对象的一个不可变版本。"""

    key: str
    version_id: str
    blob_id: str
    size: int
    checksum: str
    created_at: float
    delete_marker: bool = False  # True 表示显式删除留下的墓碑版本
    part_count: int = 0
    metadata: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "versionId": self.version_id,
            "blobId": self.blob_id,
            "size": self.size,
            "checksum": self.checksum,
            "createdAt": self.created_at,
            "deleteMarker": self.delete_marker,
            "partCount": self.part_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VersionRecord:
        return cls(
            key=data["key"],
            version_id=data["versionId"],
            blob_id=data["blobId"],
            size=data["size"],
            checksum=data["checksum"],
            created_at=data["createdAt"],
            delete_marker=data.get("deleteMarker", False),
            part_count=data.get("partCount", 0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class PartRecord:
    """一个已上传的分片。"""

    part_number: int
    blob_id: str
    size: int
    checksum: str
    etag: str
    uploaded_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "partNumber": self.part_number,
            "blobId": self.blob_id,
            "size": self.size,
            "checksum": self.checksum,
            "etag": self.etag,
            "uploadedAt": self.uploaded_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PartRecord:
        return cls(
            part_number=data["partNumber"],
            blob_id=data["blobId"],
            size=data["size"],
            checksum=data["checksum"],
            etag=data["etag"],
            uploaded_at=data["uploadedAt"],
        )


@dataclass
class UploadRecord:
    """一个进行中的分片上传会话。"""

    upload_id: str
    key: str
    part_size: int
    created_at: float
    parts: dict[int, PartRecord] = field(default_factory=dict)
    completed: bool = False
    completed_version_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "uploadId": self.upload_id,
            "key": self.key,
            "partSize": self.part_size,
            "createdAt": self.created_at,
            "parts": {str(n): p.to_dict() for n, p in sorted(self.parts.items())},
            "completed": self.completed,
            "completedVersionId": self.completed_version_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UploadRecord:
        return cls(
            upload_id=data["uploadId"],
            key=data["key"],
            part_size=data["partSize"],
            created_at=data["createdAt"],
            parts={
                int(n): PartRecord.from_dict(p) for n, p in data.get("parts", {}).items()
            },
            completed=data.get("completed", False),
            completed_version_id=data.get("completedVersionId"),
            metadata=data.get("metadata", {}),
        )
