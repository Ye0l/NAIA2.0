"""Chunked parquet reader — load a large parquet in row-batch chunks.

Startup restore and custom parquet load/merge used a single blocking
``pd.read_parquet`` of the whole file, which on a large temp/runner pool stalls
the search pool readiness (and, on low-spec machines, the whole app) while the
one long GIL-holding conversion runs. Reading in batches lets the caller report
progress and yields the GIL between batches so an asyncio event loop stays
responsive during the load. The returned DataFrame is identical to
``pd.read_parquet`` (columns/dtypes) for the index-less parquets we write
(``to_parquet(index=False)``); callers reset_index anyway.

The progress callback receives ``(loaded_rows, total_rows)`` — ``total_rows`` is
read from the file metadata up front (0 if unavailable), and is called once with
``(0, total)`` before the first batch so a UI can show a determinate bar.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
from typing import Any, Callable, Optional

from core.cpu_budget import pool_workers

ProgressCallback = Callable[[int, int], None]

# Rows per batch. 100k keeps each to_pandas() conversion short (frequent GIL
# release / progress ticks) without fragmenting into thousands of tiny frames.
DEFAULT_BATCH_ROWS = 100_000

# Rows a compact load may hold mid-decode, across all threads. This is the knob
# that trades pool-load time against peak memory: each row group in flight costs
# roughly its uncompressed parquet size again as Arrow buffers plus the pandas
# conversion. Measured on a 4,000,000-row pool in 1,048,576-row row groups
# (253MiB uncompressed each), reading row groups instead of small batches took
# 1.672s at 1.48 cores and a 2278MiB peak down to 1.391s at 1.55 cores and
# 1948MiB — faster and smaller. Adding threads then buys speed with memory:
# two row groups in flight 0.822s at 2.59 cores and 2592MiB, four 0.747s at
# 3.01 cores and 2766MiB. The default allows two of those row groups.
# Raise it with NAIA_POOL_LOAD_ROWS_IN_FLIGHT when the box has memory to spare.
DEFAULT_ROWS_IN_FLIGHT = 2_000_000
ROWS_IN_FLIGHT_ENV = "NAIA_POOL_LOAD_ROWS_IN_FLIGHT"
# Ceiling on decode threads regardless of budget and core count.
MAX_LOAD_WORKERS = 8


def _rewindable(source: Any) -> Any:
    """Normalize a source (path / bytes / file-like) to something pyarrow and
    pandas can open — re-openable across the chunked pass and the atomic
    fallback. Raw ``bytes`` are wrapped fresh each call; file-likes are seeked
    to 0; everything else is treated as a filesystem path."""
    if isinstance(source, (bytes, bytearray)):
        import io

        return io.BytesIO(source)
    if hasattr(source, "read"):
        try:
            source.seek(0)
        except Exception:
            pass
        return source
    return str(source)


def _pd_read(source: Any, columns: Optional[list[str]]):
    import pandas as pd

    return pd.read_parquet(_rewindable(source), columns=columns)


def compact_string_types_mapper():
    """Arrow ``types_mapper`` that keeps UTF-8 columns in shared Arrow buffers.

    An object-dtype conversion allocates one Python ``str`` per cell; the Arrow
    string dtype keeps the parquet UTF-8 buffer and an offset array instead, so
    concatenation and frame copies stay shallow. ``na_value=np.nan`` preserves
    the object-dtype missing-value semantics the search and result code is
    written against.
    """
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    string_dtype = pd.StringDtype(storage="pyarrow", na_value=np.nan)

    def mapper(dtype):
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            return string_dtype
        return None

    return mapper


def release_arrow_pool() -> None:
    """Hand PyArrow's freed pages back to the OS.

    PyArrow allocates through mimalloc (jemalloc on some builds), which keeps
    freed blocks in its own pool rather than returning them. After an archive
    scan that pool can hold most of a gigabyte the process no longer owns —
    ``pa.total_allocated_bytes()`` reads near zero while RSS stays high, and the
    next search allocates on top of it. Call this once a bulk read has produced
    its frame; live buffers (the frame's own columns) are never touched.
    Best-effort: a pool without the API, or an import failure, is not an error.
    """
    try:
        import pyarrow as pa

        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def parquet_total_rows(source: Path | str | bytes) -> int:
    """Best-effort row count from parquet metadata (0 if unreadable)."""
    try:
        import pyarrow.parquet as pq

        meta = pq.ParquetFile(_rewindable(source)).metadata
        return int(meta.num_rows) if meta is not None else 0
    except Exception:
        return 0


def _rows_in_flight_budget() -> int:
    raw = os.environ.get(ROWS_IN_FLIGHT_ENV, "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return DEFAULT_ROWS_IN_FLIGHT


def _row_group_plan(parquet_file: Any, source: Any) -> tuple[int, int]:
    """(row groups, decode threads) for a row-group read, or (0, 0) to stream.

    Streaming stays the answer when a single row group is larger than the whole
    in-flight budget — reading it whole would be the unbounded allocation this
    module exists to avoid — and when the source is not a filesystem path, since
    each worker needs its own reader and a BytesIO cannot be shared.
    """
    metadata = getattr(parquet_file, "metadata", None)
    groups = int(getattr(metadata, "num_row_groups", 0) or 0)
    if groups <= 0:
        return 0, 0
    try:
        widest = max(metadata.row_group(i).num_rows for i in range(groups))
    except Exception:
        return 0, 0
    budget = _rows_in_flight_budget()
    if widest <= 0 or widest > budget:
        return 0, 0
    if not isinstance(source, str):
        return groups, 1
    return groups, pool_workers(min(groups, budget // widest), cap=MAX_LOAD_WORKERS)


def _read_row_groups(
    source: str,
    parquet_file: Any,
    groups: int,
    workers: int,
    columns: Optional[list[str]],
    mapper: Any,
    total: int,
    tick: Callable[[int, int], None],
) -> list[Any]:
    """Decode row groups into frames, in order, optionally several at a time."""
    import pyarrow.parquet as pq

    frames: list[Any] = [None] * groups

    def _one(index: int, handle: Any = None) -> Any:
        # A ParquetFile is not safe to read from several threads, so each worker
        # opens its own. The sequential path reuses the caller's handle.
        reader = handle if handle is not None else pq.ParquetFile(source)
        table = reader.read_row_group(index, columns=columns) if columns is not None \
            else reader.read_row_group(index)
        return table.to_pandas(types_mapper=mapper)

    loaded = 0
    if workers <= 1:
        for index in range(groups):
            frames[index] = _one(index, parquet_file)
            loaded += len(frames[index])
            tick(loaded, total or loaded)
        return frames

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pool-load") as executor:
        futures = {executor.submit(_one, index): index for index in range(groups)}
        for future in as_completed(futures):
            index = futures[future]
            frames[index] = future.result()
            loaded += len(frames[index])
            tick(loaded, total or loaded)
    return frames


def read_parquet_chunked(
    path: Path | str | bytes,
    *,
    progress: Optional[ProgressCallback] = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    columns: Optional[list[str]] = None,
    compact_strings: bool = False,
) -> Any:
    """Read a parquet source (filesystem path, raw ``bytes``, or file-like) in
    row batches, returning one concatenated DataFrame.

    Legacy mode falls back to ``pd.read_parquet`` on reader failures. Compact
    mode requires PyArrow and never retries a failed read with a whole-file
    allocation. ``compact_strings`` uses shared Arrow UTF-8 buffers (missing
    strings become NaN); other column types retain their normal conversion.
    ``progress(loaded, total)`` is
    invoked after ``(0, total)`` and once per batch; callback errors are
    swallowed so a UI hiccup never fails the load.
    """
    def _tick(loaded: int, total: int) -> None:
        if progress is None:
            return
        try:
            progress(loaded, total)
        except Exception:
            pass

    try:
        import pyarrow.parquet as pq
    except Exception:
        if compact_strings:
            raise
        frame = _pd_read(path, columns)
        _tick(len(frame), len(frame))
        return frame

    rewindable = _rewindable(path)
    try:
        parquet_file = pq.ParquetFile(rewindable)
    except Exception:
        if compact_strings:
            raise
        frame = _pd_read(path, columns)
        _tick(len(frame), len(frame))
        return frame

    total = int(parquet_file.metadata.num_rows) if parquet_file.metadata is not None else 0
    _tick(0, total)

    # Arrow-backed strings keep UTF-8 buffers shared across concatenation and
    # snapshot copies instead of allocating a Python object per cell. Numeric
    # and nested columns retain the existing conversion semantics.
    mapper = compact_string_types_mapper() if compact_strings else None

    # Whole row groups beat a stream of small batches on both axes — fewer,
    # larger pieces to concatenate — and they are the unit that can be decoded on
    # several threads. _row_group_plan declines when a row group would not fit
    # the in-flight budget or the source cannot be reopened per worker, and the
    # batch stream below stays the answer for those.
    groups, workers = _row_group_plan(parquet_file, rewindable)
    if groups > 0:
        try:
            frames = _read_row_groups(
                rewindable, parquet_file, groups, workers, columns, mapper, total, _tick,
            )
        except MemoryError:
            raise
        except Exception:
            if compact_strings:
                raise
            frame = _pd_read(path, columns)
            _tick(len(frame), len(frame))
            return frame
        else:
            frames = [frame for frame in frames if frame is not None]
            if frames:
                try:
                    if len(frames) == 1:
                        return frames[0].reset_index(drop=True)
                    import pandas as pd

                    return pd.concat(frames, ignore_index=True)
                finally:
                    frames.clear()
                    if compact_strings:
                        release_arrow_pool()

    frames: list[Any] = []
    loaded = 0
    try:
        iterator = (
            parquet_file.iter_batches(batch_size=batch_rows, columns=columns)
            if columns is not None
            else parquet_file.iter_batches(batch_size=batch_rows)
        )
        for batch in iterator:
            frames.append(batch.to_pandas(types_mapper=mapper))
            loaded += batch.num_rows
            _tick(loaded, total or loaded)
    except MemoryError:
        # Retrying a whole-file read under memory pressure makes the peak worse.
        raise
    except Exception:
        if compact_strings:
            raise
        frames.clear()
        # Any mid-stream failure → fall back to the atomic read so callers still
        # get a correct frame (progress just won't be granular for the retry).
        frame = _pd_read(path, columns)
        _tick(len(frame), len(frame))
        return frame

    if not frames:
        frame = _pd_read(path, columns)
        _tick(len(frame), len(frame))
        return frame
    try:
        if len(frames) == 1:
            return frames[0].reset_index(drop=True)
        import pandas as pd

        return pd.concat(frames, ignore_index=True)
    finally:
        # The per-batch Arrow buffers are dead once the frames are built/joined.
        if compact_strings:
            release_arrow_pool()


# ── Search-pool load progress broadcast ──────────────────────────────────────
# Progress is surfaced to the Remote Web clients as ``search_loading`` WS
# payloads published over the context event bus (a headless_routes bridge
# broadcasts them to every client). A load smaller than the threshold stays
# silent so a fast pool never flashes the Tag/Tag-Filter lock overlay.
SEARCH_LOADING_EVENT = "search_pool_broadcast"
DEFAULT_LOADING_THRESHOLD = 200_000  # rows


def _publish(context: Any, payload: dict) -> None:
    try:
        context.publish(SEARCH_LOADING_EVENT, payload)
    except Exception:
        pass


def _set_loading_state(context: Any, active: bool, loaded: int, total: int) -> None:
    try:
        context._search_pool_loading = {
            "active": bool(active),
            "loaded": int(loaded),
            "total": int(total),
        }
    except Exception:
        pass


def make_search_load_progress(
    context: Any,
    *,
    threshold: int = DEFAULT_LOADING_THRESHOLD,
) -> tuple[ProgressCallback, Callable[[], None]]:
    """Build ``(progress, done)`` for an announced chunked pool load.

    ``progress(loaded, total)`` is handed to ``read_parquet_chunked``; on the
    first tick it decides — from ``total`` — whether the load is big enough to
    announce (lock + progress) or stays silent. ``done()`` MUST be called once
    after the frame is installed (or the load is abandoned) to clear the loading
    flag and release the client lock. Both are exception-safe.
    """
    # Route through the context's shared depth-counted pool-loading ownership so a
    # parquet LOAD and a concurrent tag-filter FILTER can't clear each other's
    # loading flag (they share one counter; the flag stays until BOTH end). Fall
    # back to direct state writes for contexts without the ownership API (test
    # doubles). Crucially, a silent (small) load NEVER touches the flag now, so it
    # can't clobber a concurrently-active filter.
    use_owner = hasattr(context, "pool_loading_begin")
    state = {"announced": False, "silent": False}

    def _begin(total: int) -> None:
        if use_owner:
            context.pool_loading_begin("load")
            context.pool_loading_progress("load", 0, int(total or 0))
        else:
            _set_loading_state(context, True, 0, total)
            _publish(context, {"type": "search_loading", "loading": True, "loaded": 0, "total": int(total or 0)})

    def _tick(loaded: int, total: int) -> None:
        if use_owner:
            context.pool_loading_progress("load", int(loaded), int(total or loaded))
        else:
            _set_loading_state(context, True, loaded, total)
            _publish(context, {"type": "search_loading", "loading": True, "loaded": int(loaded), "total": int(total or loaded)})

    def _finish() -> None:
        if use_owner:
            context.pool_loading_end()
        else:
            _set_loading_state(context, False, 0, 0)
            _publish(context, {"type": "search_loading", "loading": False})

    def progress(loaded: int, total: int) -> None:
        if state["silent"]:
            return
        if not state["announced"]:
            state["announced"] = True
            if total and total < threshold:
                state["silent"] = True
                return
            _begin(total)
            return
        _tick(loaded, total)

    def done() -> None:
        # Only the announced (non-silent) load owns a begin, so only it ends. A
        # silent load leaves the shared flag untouched (no clobber).
        if state["announced"] and not state["silent"]:
            _finish()

    return progress, done


__all__ = [
    "DEFAULT_ROWS_IN_FLIGHT",
    "ROWS_IN_FLIGHT_ENV",
    "compact_string_types_mapper",
    "release_arrow_pool",
    "read_parquet_chunked",
    "parquet_total_rows",
    "make_search_load_progress",
    "DEFAULT_BATCH_ROWS",
    "SEARCH_LOADING_EVENT",
    "DEFAULT_LOADING_THRESHOLD",
]
