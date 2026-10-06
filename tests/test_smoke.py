"""冒烟测试：最小骨架可用性。"""


def test_skeleton_imports():
    import object_storage  # noqa: F401
    from object_storage.service import ObjectStorageService  # noqa: F401
