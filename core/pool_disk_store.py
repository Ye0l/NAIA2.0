"""Disk-backed search pool — keep the pool's bulk in a memory-mapped file.

The search pool is a pandas DataFrame of millions of posts, and almost all of
its weight is tag text: on a 2,000,000-row pool the frame is ~1.4GiB of which
~1.15GiB is UTF-8 strings, and the tag-filter index adds a second lowercased
copy of the same text. Held on the Python heap that is anonymous memory: the
kernel cannot drop it, so a pool bigger than the container's limit is an OOM
kill rather than a slowdown.

This module moves that bulk to disk without changing what callers see. A frame
is written once, uncompressed, as an Arrow IPC file and reopened through
``mmap``; its string columns come back as the same Arrow-backed pandas string
dtype the loaders already produce, but their buffers now point into the mapped
file. Those pages are page cache, not heap — the kernel reads them on demand
and evicts them under pressure, so a pool larger than RAM costs disk reads
instead of the process. Numeric columns are small (8 bytes a row) and come back
as ordinary writable numpy arrays, so every existing ``frame[...]``/``.loc``
path keeps working unchanged.

Every producer that would otherwise materialize a whole pool in RAM — a parquet
load, an archive search, a filtered subset — writes it here in bounded chunks,
so the peak while building is one chunk, not the pool.

On POSIX the file is unlinked as soon as it is mapped: the mapping keeps the
data alive and the space comes back when the last frame referencing it is
gone, so nothing is left behind even after a crash. Windows cannot delete a
mapped file, so there each process writes into its own directory and the
directories of processes that are gone are removed at startup.

Knobs (environment):
    NAIA_POOL_ON_DISK=0           keep the pool in RAM (the previous behaviour)
    NAIA_POOL_DISK_DIR=<path>     where spill files go (default: cache dir)
    NAIA_POOL_DISK_MIN_ROWS=<n>   smaller frames stay in RAM (default 100000)

Every helper is best-effort: on any failure (disk full, an unconvertible
column, no pyarrow) it returns ``None`` and the caller keeps its in-memory
path, so turning this on can never make a load fail that used to succeed.
"""

from __future__ import annotations

import itertools
import os
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

ENABLE_ENV = "NAIA_POOL_ON_DISK"
DIR_ENV = "NAIA_POOL_DISK_DIR"
MIN_ROWS_ENV = "NAIA_POOL_DISK_MIN_ROWS"

DEFAULT_MIN_ROWS = 100_000
# Rows converted and written per step. Bounds the transient RAM of a spill to
# one chunk's worth of strings (~150MiB at the tag density measured), while
# keeping the IPC record batches large enough that reads stay sequential.
CHUNK_ROWS = 262_144
# Spare room demanded on the target disk beyond the estimated file size.
FREE_SPACE_SLACK = 1.10

_lock = threading.Lock()
_configured_root: Optional[Path] = None
_process_dir: Optional[Path] = None
_counter = itertools.count()
# Windows only: (start, end, path) of live mappings. POSIX reads the kernel's
# own map list instead, which is exact and needs no bookkeeping.
_regions: list[tuple[int, int, Path]] = []


def _log(message: str) -> None:
    try:
        from core.safe_console import safe_print

        safe_print(message)
    except Exception:
        try:
            print(message)
        except Exception:
            pass


# ── configuration ───────────────────────────────────────────────────────────

def enabled() -> bool:
    raw = os.environ.get(ENABLE_ENV, "").strip().lower()
    return raw not in ("0", "false", "no", "off")


def min_rows() -> int:
    raw = os.environ.get(MIN_ROWS_ENV, "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            pass
    return DEFAULT_MIN_ROWS


def wants_disk(rows: int) -> bool:
    """Whether a frame of ``rows`` rows should live on disk."""
    return enabled() and int(rows or 0) >= max(1, min_rows())


def configure(root: Path | str | None) -> None:
    """Set the default spill root (the runtime cache dir). ``NAIA_POOL_DISK_DIR``
    still wins. Safe to call more than once; only an unused root is replaced."""
    global _configured_root, _process_dir
    if root is None:
        return
    with _lock:
        new_root = Path(root)
        if _configured_root == new_root:
            return
        _configured_root = new_root
        if _process_dir is not None and not any(_process_dir.iterdir() if _process_dir.is_dir() else []):
            _process_dir = None


def _root() -> Path:
    env = os.environ.get(DIR_ENV, "").strip()
    if env:
        return Path(env)
    if _configured_root is not None:
        return _configured_root
    user_data = os.environ.get("NAIA_USER_DATA_DIR", "").strip()
    if user_data:
        return Path(user_data) / "cache" / "pool_spill"
    return Path(tempfile.gettempdir()) / "naia_pool_spill"


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":
        # Deleting a live process's mapped files fails on Windows, which is
        # exactly the guard we need, so report "gone" and let rmtree try.
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _spill_dir() -> Path:
    """This process's spill directory, created (and swept) on first use."""
    global _process_dir
    with _lock:
        if _process_dir is not None:
            return _process_dir
        root = _root()
        root.mkdir(parents=True, exist_ok=True)
        mine = root / f"p{os.getpid()}"
        for entry in root.glob("p*"):
            if not entry.is_dir():
                continue
            try:
                pid = int(entry.name[1:])
            except ValueError:
                continue
            # Our own directory is stale too: a container restarts as pid 1
            # again, and a fresh process has nothing mapped yet.
            if entry == mine or not _pid_alive(pid):
                shutil.rmtree(entry, ignore_errors=True)
        mine.mkdir(parents=True, exist_ok=True)
        _process_dir = mine
        return mine


def _new_path(tag: str) -> Path:
    return _spill_dir() / f"{tag}-{next(_counter):06d}.arrow"


def _sweep_unmapped() -> None:
    """Windows: delete spill files whose frames are gone (the delete fails
    while a mapping is alive). POSIX files are unlinked at map time."""
    if os.name != "nt" or _process_dir is None:
        return
    with _lock:
        for path in list(_process_dir.glob("*.arrow")):
            try:
                path.unlink()
            except OSError:
                continue
            _regions[:] = [r for r in _regions if r[2] != path]


# ── disk-backed detection ───────────────────────────────────────────────────

def _proc_regions() -> Optional[list[tuple[int, int]]]:
    """Address ranges of this process's live spill mappings (Linux), or
    ``None`` where /proc is unavailable."""
    if _process_dir is None:
        return []
    marker = str(_process_dir)
    try:
        with open("/proc/self/maps", "r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return None
    out = []
    for line in lines:
        if marker not in line:
            continue
        span = line.split(" ", 1)[0]
        start, _, end = span.partition("-")
        try:
            out.append((int(start, 16), int(end, 16)))
        except ValueError:
            continue
    return out


def _live_regions() -> list[tuple[int, int]]:
    regions = _proc_regions()
    if regions is not None:
        return regions
    with _lock:
        return [(start, end) for start, end, _ in _regions]


def _arrow_chunks(series: Any):
    values = getattr(getattr(series, "array", None), "_pa_array", None)
    if values is None:
        return None
    return getattr(values, "chunks", [values])


def _string_like(series: Any) -> bool:
    dtype = series.dtype
    if dtype == object:
        return True
    try:
        import pandas as pd

        if isinstance(dtype, pd.StringDtype):
            return True
        if isinstance(dtype, pd.ArrowDtype):
            import pyarrow as pa

            return pa.types.is_string(dtype.pyarrow_dtype) or pa.types.is_large_string(dtype.pyarrow_dtype)
    except Exception:
        return False
    return False


def is_disk_backed(frame: Any) -> bool:
    """True when every string column's buffers live in a spill mapping (or the
    frame has no string columns, so there is nothing a spill would move)."""
    if frame is None or getattr(frame, "empty", True):
        return True
    regions = None
    for name in frame.columns:
        series = frame[name]
        if not _string_like(series):
            continue
        chunks = _arrow_chunks(series)
        if chunks is None:
            return False  # object dtype: Python strings on the heap
        if regions is None:
            regions = _live_regions()
            if not regions:
                return False
        for chunk in chunks:
            buffers = chunk.buffers()
            data = buffers[2] if len(buffers) > 2 else None
            if data is None or data.size == 0:
                continue
            address = data.address
            if not any(start <= address < end for start, end in regions):
                return False
    return True


def needs_spill(frame: Any) -> bool:
    if frame is None or getattr(frame, "empty", True):
        return False
    if not wants_disk(len(frame)):
        return False
    return not is_disk_backed(frame)


# ── writing ─────────────────────────────────────────────────────────────────

class SpillError(RuntimeError):
    pass


def _normalized_schema(schema: Any) -> Any:
    """Strings become ``large_string`` (what the pandas Arrow string dtype holds
    internally, so mapping it back is zero-copy) and all-null columns too, so a
    later chunk with real text still fits the file's schema."""
    import pyarrow as pa

    fields = []
    for field in schema:
        if pa.types.is_string(field.type) or pa.types.is_null(field.type):
            field = field.with_type(pa.large_string())
        fields.append(field)
    return pa.schema(fields, metadata=schema.metadata)


def _mapper():
    from core.parquet_chunk_loader import compact_string_types_mapper

    return compact_string_types_mapper()


class SpillWriter:
    """Append record batches to one IPC file, then map it back.

    ``write_frame``/``write_table`` take chunks with the same columns; the first
    chunk fixes the schema. ``finish()`` returns the mapped ``pa.Table``.
    """

    def __init__(self, tag: str = "pool", expected_bytes: int = 0):
        import pyarrow as pa

        self._pa = pa
        self.path = _new_path(tag)
        if expected_bytes > 0:
            try:
                free = shutil.disk_usage(self.path.parent).free
            except OSError:
                free = 0
            if free and free < expected_bytes * FREE_SPACE_SLACK:
                raise SpillError(
                    f"not enough disk for the pool spill ({expected_bytes / 2**20:.0f}MiB "
                    f"needed, {free / 2**20:.0f}MiB free at {self.path.parent})"
                )
        self._sink = pa.OSFile(str(self.path), "wb")
        self._writer = None
        self.schema = None
        self.rows = 0

    def _open(self, schema: Any) -> None:
        import pyarrow.ipc as ipc

        self.schema = _normalized_schema(schema)
        self._writer = ipc.new_file(self._sink, self.schema)

    def write_table(self, table: Any) -> None:
        if self._writer is None:
            self._open(table.schema)
        if table.schema != self.schema:
            try:
                table = table.cast(self.schema)
            except Exception as exc:
                raise SpillError(f"chunk does not fit the pool schema: {exc}") from exc
        for batch in table.to_batches():
            self._writer.write_batch(batch)
        self.rows += table.num_rows

    def write_frame(self, chunk: Any) -> None:
        pa = self._pa
        try:
            if self.schema is not None:
                try:
                    table = pa.Table.from_pandas(chunk, schema=self.schema, preserve_index=False)
                except Exception:
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
            else:
                table = pa.Table.from_pandas(chunk, preserve_index=False)
        except Exception as exc:
            raise SpillError(f"column cannot be written to Arrow: {exc}") from exc
        self.write_table(table)

    def finish(self) -> Any:
        import pyarrow.ipc as ipc

        pa = self._pa
        if self._writer is None:
            raise SpillError("nothing was written")
        self._writer.close()
        self._sink.close()
        mapped = pa.memory_map(str(self.path), "r")
        size = mapped.size()
        whole = mapped.read_buffer(size)
        table = ipc.open_file(pa.BufferReader(whole)).read_all()
        if os.name == "nt":
            with _lock:
                _regions.append((whole.address, whole.address + size, self.path))
        else:
            # The mapping keeps the bytes alive; the name is no longer needed
            # and the space is returned when the last frame using it is freed.
            try:
                self.path.unlink()
            except OSError:
                pass
        return table

    def abort(self) -> None:
        try:
            if self._writer is not None:
                self._writer.close()
        except Exception:
            pass
        try:
            self._sink.close()
        except Exception:
            pass
        try:
            self.path.unlink()
        except OSError:
            pass


def _release() -> None:
    try:
        from core.parquet_chunk_loader import release_arrow_pool

        release_arrow_pool()
    except Exception:
        pass


def frame_from_table(table: Any, index: Any = None) -> Any:
    """pandas view of a mapped table: strings stay in the mapping, numerics
    become ordinary (writable) numpy columns."""
    frame = table.to_pandas(types_mapper=_mapper())
    if index is not None:
        frame.index = index
    return frame


def _plain_columns(frame: Any) -> bool:
    columns = list(frame.columns)
    return all(isinstance(name, str) for name in columns) and len(set(columns)) == len(columns)


def _estimate_bytes(frame: Any) -> int:
    try:
        return int(frame.memory_usage(index=False, deep=False).sum())
    except Exception:
        return 0


def _default_index(index: Any, length: int) -> bool:
    try:
        import pandas as pd

        return (
            isinstance(index, pd.RangeIndex)
            and index.start == 0
            and index.step == 1
            and len(index) == length
        )
    except Exception:
        return False


def _write_chunks(frame: Any, chunks: Iterable[Any], tag: str, estimate: int, index: Any) -> Optional[Any]:
    _sweep_unmapped()
    writer = None
    try:
        writer = SpillWriter(tag, expected_bytes=estimate)
        for chunk in chunks:
            if len(chunk):
                writer.write_frame(chunk)
        if writer.rows == 0:
            writer.abort()
            return None
        table = writer.finish()
        result = frame_from_table(table, index)
        if list(result.columns) != list(frame.columns):
            return None
        return result
    except MemoryError:
        if writer is not None:
            writer.abort()
        raise
    except Exception as exc:
        if writer is not None:
            writer.abort()
        _log(f"⚠️ pool disk spill skipped ({tag}): {exc}")
        return None
    finally:
        _release()


def spill_frame(frame: Any, *, tag: str = "pool", force: bool = False) -> Optional[Any]:
    """A disk-backed copy of ``frame`` (same columns, rows, index), or ``None``
    when the frame should stay in RAM or the spill failed."""
    if frame is None or getattr(frame, "empty", True):
        return None
    if not force and not needs_spill(frame):
        return None
    if not _plain_columns(frame):
        return None
    n = len(frame)
    index = None if _default_index(frame.index, n) else frame.index
    chunks = (frame.iloc[start:start + CHUNK_ROWS] for start in range(0, n, CHUNK_ROWS))
    return _write_chunks(frame, chunks, tag, _estimate_bytes(frame), index)


def _gather(frame: Any, idx: Any) -> Any:
    """``frame.iloc[idx]`` that only ever touches a bounded row range at once.

    A take on a multi-chunk Arrow column concatenates the chunks it reads, so a
    gather whose positions span the pool rebuilt every column on the heap, per
    output chunk. Positions are grouped by source window (a zero-copy slice),
    gathered window by window, and put back in the requested order.
    """
    import numpy as np

    if len(idx) == 0:
        return frame.iloc[idx]
    lo = int(idx.min())
    hi = int(idx.max())
    if hi - lo < 2 * CHUNK_ROWS:
        return frame.iloc[lo:hi + 1].iloc[idx - lo]
    import pandas as pd

    window = idx // CHUNK_ROWS
    order = np.argsort(window, kind="stable")
    ordered = idx[order]
    ordered_window = window[order]
    bounds = np.flatnonzero(np.diff(ordered_window)) + 1
    pieces = []
    for segment in np.split(ordered, bounds):
        start = int(segment[0] // CHUNK_ROWS) * CHUNK_ROWS
        pieces.append(frame.iloc[start:start + CHUNK_ROWS].iloc[segment - start])
    combined = pieces[0] if len(pieces) == 1 else pd.concat(pieces)
    inverse = np.empty_like(order)
    inverse[order] = np.arange(len(order))
    return combined.iloc[inverse]


def take_frame(frame: Any, positions: Any, *, tag: str = "subset") -> Any:
    """``frame.iloc[positions]`` as an independent frame, on disk when large.

    Equivalent to ``frame.iloc[positions].copy()`` (index labels kept), but the
    rows are gathered and written one chunk at a time, so a multi-million-row
    subset never sits in RAM at once.
    """
    import numpy as np

    positions = np.asarray(positions, dtype=np.int64)
    if not wants_disk(len(positions)) or not _plain_columns(frame):
        return frame.iloc[positions].copy()
    total = max(1, len(frame))
    estimate = int(_estimate_bytes(frame) * (len(positions) / total))
    index = frame.index.take(positions)
    if _default_index(index, len(positions)):
        index = None
    chunks = (
        _gather(frame, positions[start:start + CHUNK_ROWS])
        for start in range(0, len(positions), CHUNK_ROWS)
    )
    result = _write_chunks(frame, chunks, tag, estimate, index)
    if result is None:
        return frame.iloc[positions].copy()
    return result


def subset_frame(frame: Any, mask: Any, *, tag: str = "subset") -> Any:
    """``frame[mask].copy()`` for a boolean row mask, on disk when large."""
    import numpy as np

    values = getattr(mask, "to_numpy", None)
    array = values(dtype=bool, na_value=False) if callable(values) else np.asarray(mask, dtype=bool)
    return take_frame(frame, np.flatnonzero(array), tag=tag)


def spill_chunked_array(arrays: Iterable[Any], *, tag: str = "index") -> Optional[Any]:
    """Write a stream of Arrow arrays (one column) and return the mapped
    ``ChunkedArray``, or ``None`` on failure."""
    import pyarrow as pa

    _sweep_unmapped()
    writer = None
    try:
        writer = SpillWriter(tag)
        for array in arrays:
            if len(array):
                writer.write_table(pa.table({"v": array}))
        if writer.rows == 0:
            writer.abort()
            return None
        return writer.finish().column("v")
    except MemoryError:
        if writer is not None:
            writer.abort()
        raise
    except Exception as exc:
        if writer is not None:
            writer.abort()
        _log(f"⚠️ pool disk spill skipped ({tag}): {exc}")
        return None
    finally:
        _release()


def share_frame(frame: Any) -> Any:
    """An independent handle on ``frame`` without duplicating its columns where
    pandas can guarantee independence (copy-on-write: pandas 3, or the option
    enabled on 2.x). Otherwise a normal deep copy — the existing semantics."""
    if frame is None:
        return None
    try:
        import pandas as pd

        cow = bool(pd.options.mode.copy_on_write)
    except Exception:
        cow = False
    if not cow:
        try:
            import pandas as pd

            cow = int(pd.__version__.split(".", 1)[0]) >= 3
        except Exception:
            cow = False
    return frame.copy(deep=not cow)


__all__ = [
    "ENABLE_ENV",
    "DIR_ENV",
    "MIN_ROWS_ENV",
    "CHUNK_ROWS",
    "SpillError",
    "SpillWriter",
    "configure",
    "enabled",
    "frame_from_table",
    "is_disk_backed",
    "min_rows",
    "needs_spill",
    "share_frame",
    "spill_chunked_array",
    "spill_frame",
    "subset_frame",
    "take_frame",
    "wants_disk",
]
