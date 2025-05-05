import orjson

import asyncio
from functools import partial
from typing import Any, Callable, Optional
from fsspec import url_to_fs, AbstractFileSystem

from microkit.config import settings


class AsyncFS:
    """
    Async wrapper around any fsspec-backed filesystem. Provides generic read/write
    APIs where you supply serializers/deserializers, plus convenience sugar.

    Core API:
        async def write(path: str, obj: Any, serializer: Callable[[Any], bytes])
        async def read(path: str, deserializer: Callable[[bytes], Any]) -> Any

    Convenience:
        write_bytes, read_bytes, write_text, read_text, write_json, read_json

    Usage:
        async_fs = AsyncFileSystem()
        # Generic
        await async_fs.write('foo.bin', b'data', lambda d: d)
        data = await async_fs.read('foo.bin', lambda b: b)
    """

    def __init__(self, storage_url: Optional[str] = None):
        url = storage_url or settings.raw_storage_url
        fs, root = url_to_fs(url, anon=False)
        self._fs: AbstractFileSystem = fs
        self._root: str = root.rstrip("/")

    @property
    def base_path(self) -> str:
        return self._root

    async def _run(self, func: Callable, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    # Core generic API
    async def write(
        self, path: str, obj: Any, serializer: Callable[[Any], bytes]
    ) -> None:
        data = serializer(obj)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("serializer must return bytes")
        full = f"{self._root}/{path.lstrip('/')}"
        # ensure parent directory exists
        parent = full.rsplit("/", 1)[0]
        await self._run(self._fs.makedirs, parent, exist_ok=True)
        # write data
        await self._run(lambda p, d: self._fs.open(p, "wb").write(d), full, data)

    async def read(self, path: str, deserializer: Callable[[bytes], Any]) -> Any:
        src = path if not _is_relative(path) else f"{self._root}/{path.lstrip('/')}"
        data = await self._run(lambda p: self._fs.open(p, "rb").read(), src)
        return deserializer(data)

    # --- Copy across file systems ---
    async def copy(self, source_path: str, dest_path: str) -> None:
        """
        Copy a single file from source_path to dest_path. Supports cross-FS.
        If paths are relative, they use this filesystem's base. Otherwise, full URL.
        """
        # resolve source filesystem and path
        if _is_relative(source_path):
            src_fs = self._fs
            src = f"{self._root}/{source_path.lstrip('/')}"
        else:
            src_fs, src = url_to_fs(source_path, anon=False)
        # resolve destination filesystem and path
        if _is_relative(dest_path):
            dst_fs = self._fs
            dst = f"{self._root}/{dest_path.lstrip('/')}"
        else:
            dst_fs, dst = url_to_fs(dest_path, anon=False)
        # ensure parent directory on destination
        parent = dst.rsplit("/", 1)[0]
        await self._run(dst_fs.makedirs, parent, exist_ok=True)

        # perform streaming copy
        def _stream_copy():
            with src_fs.open(src, "rb") as fin, dst_fs.open(dst, "wb") as fout:
                for chunk in iter(lambda: fin.read(1024 * 1024), b""):
                    fout.write(chunk)

        await self._run(_stream_copy)

        # --- Convenience wrappers for reading and writing types ---

    async def write_json(self, path: str, obj: dict):
        await self.write(path, obj, lambda o: orjson.dumps(o))

    async def read_json(self, path: str) -> dict:
        return await self.read(path, lambda o: orjson.loads(o))

    async def write_bytes(self, path: str, obj: bytes):
        await self.write(path, obj, lambda o: o)

    async def read_bytes(self, path: str) -> bytes:
        return await self.read(path, lambda o: o)


class SyncFS:
    """
    Synchronous wrapper around any fsspec-backed filesystem. Mirrors AsyncFileSystem API.
    """

    def __init__(self, storage_url: Optional[str] = None):
        url = storage_url or settings.raw_storage_url
        fs, root = url_to_fs(url, anon=False)
        self._fs: AbstractFileSystem = fs
        self._root: str = root.rstrip("/")

    @property
    def base_path(self) -> str:
        return self._root

    # Core generic API
    def write(self, path: str, obj: Any, serializer: Callable[[Any], bytes]) -> None:
        data = serializer(obj)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("serializer must return bytes")
        full = f"{self._root}/{path.lstrip('/')}"
        with self._fs.open(full, "wb") as f:
            f.write(data)

    def read(self, path: str, deserializer: Callable[[bytes], Any]) -> Any:
        full = f"{self._root}/{path.lstrip('/')}"
        with self._fs.open(full, "rb") as f:
            data = f.read()
        return deserializer(data)

    def copy(self, source_path: str, dest_path: str, **kwargs) -> None:
        """
        Copy a file from source_path to dest_path within the same or different filesystem.
        Paths are relative to the base root. Additional kwargs are passed to fsspec's copy.
        """
        src_full = f"{self._root}/{source_path.lstrip('/')}"
        dst_full = f"{self._root}/{dest_path.lstrip('/')}"
        self._fs.copy(src_full, dst_full, **kwargs)


def _is_relative(path: str) -> bool:
    return "://" not in path
