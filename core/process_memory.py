"""How much memory the app is using — for the toolbar memory indicator.

The number that matters is the **heap** (anonymous memory): it is what the
kernel cannot drop, so it is what runs a container into its limit. Total RSS is
reported too, but since large search pools are served from memory-mapped files
(core/pool_disk_store.py) RSS also counts page cache the kernel evicts at will,
and reading it as "usage" would overstate it by the size of the pool.

Sources, best first:
- Linux: /proc/self/status (RssAnon / RssFile / VmHWM) plus the memory cgroup
  limit, which is the limit a container actually runs into.
- Windows: GetProcessMemoryInfo (private bytes / working set).
- elsewhere: psutil when installed, else only the peak from getrusage.

Every field is optional; the UI shows what it gets.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional

_UNLIMITED = 1 << 60  # cgroup v1 reports "no limit" as a huge number


def _read(path: str | Path) -> Optional[str]:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None


def _linux_process() -> dict[str, int]:
    text = _read("/proc/self/status")
    if not text:
        return {}
    fields = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        parts = rest.split()
        if len(parts) >= 2 and parts[1].lower() == "kb":
            try:
                fields[key] = int(parts[0]) * 1024
            except ValueError:
                pass
    out = {}
    if "VmRSS" in fields:
        out["rss"] = fields["VmRSS"]
    if "RssAnon" in fields:
        out["heap"] = fields["RssAnon"]
    if "RssFile" in fields:
        out["file"] = fields["RssFile"]
    if "VmHWM" in fields:
        out["peak_rss"] = fields["VmHWM"]
    return out


def _cgroup_dirs() -> tuple[Optional[Path], Optional[Path]]:
    """(cgroup v2 dir, cgroup v1 memory dir) of this process, where present."""
    text = _read("/proc/self/cgroup") or ""
    v2 = v1 = None
    for line in text.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        _, controllers, rel = parts
        rel = rel.lstrip("/")
        if controllers == "":
            candidate = Path("/sys/fs/cgroup") / rel
            if (candidate / "memory.max").exists():
                v2 = candidate
        elif "memory" in controllers.split(","):
            for base in (Path("/sys/fs/cgroup/memory") / rel, Path("/sys/fs/cgroup/memory")):
                if (base / "memory.limit_in_bytes").exists():
                    v1 = base
                    break
    if v2 is None and (Path("/sys/fs/cgroup") / "memory.max").exists():
        v2 = Path("/sys/fs/cgroup")
    return v2, v1


def _cgroup_limits() -> dict[str, int]:
    v2, v1 = _cgroup_dirs()
    out: dict[str, int] = {}

    def _int(value: Optional[str]) -> Optional[int]:
        if not value or value == "max":
            return None
        try:
            number = int(value)
        except ValueError:
            return None
        return number if 0 < number < _UNLIMITED else None

    if v2 is not None:
        limit = _int(_read(v2 / "memory.max"))
        usage = _int(_read(v2 / "memory.current"))
    elif v1 is not None:
        limit = _int(_read(v1 / "memory.limit_in_bytes"))
        usage = _int(_read(v1 / "memory.usage_in_bytes"))
    else:
        return out
    if limit:
        out["limit"] = limit
    if usage:
        out["limit_usage"] = usage
    return out


def _linux_system() -> dict[str, int]:
    text = _read("/proc/meminfo") or ""
    fields = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        parts = rest.split()
        if parts:
            try:
                fields[key] = int(parts[0]) * 1024
            except ValueError:
                pass
    out = {}
    if "MemTotal" in fields:
        out["system_total"] = fields["MemTotal"]
    if "MemAvailable" in fields:
        out["system_available"] = fields["MemAvailable"]
    return out


def _windows_process() -> dict[str, int]:
    try:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
                ("PrivateUsage", ctypes.c_size_t),
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(Counters)
        process = ctypes.windll.kernel32.GetCurrentProcess()
        if not ctypes.windll.psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
            return {}
        out = {
            "rss": int(counters.WorkingSetSize),
            "heap": int(counters.PrivateUsage),
            "peak_rss": int(counters.PeakWorkingSetSize),
        }

        class Status(ctypes.Structure):
            _fields_ = [
                ("dwLength", wintypes.DWORD),
                ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = Status()
        status.dwLength = ctypes.sizeof(Status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            out["system_total"] = int(status.ullTotalPhys)
            out["system_available"] = int(status.ullAvailPhys)
        return out
    except Exception:
        return {}


def _generic_process() -> dict[str, int]:
    try:
        import psutil  # type: ignore

        process = psutil.Process()
        info = process.memory_info()
        out = {"rss": int(info.rss)}
        system = psutil.virtual_memory()
        out["system_total"] = int(system.total)
        out["system_available"] = int(system.available)
        return out
    except Exception:
        pass
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # ru_maxrss is bytes on macOS and kilobytes elsewhere.
        return {"peak_rss": int(peak if sys.platform == "darwin" else peak * 1024)}
    except Exception:
        return {}


def _pool_info(context: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        results = getattr(context, "search_results", None)
        if results is not None:
            out["pool_rows"] = int(results.get_count())
    except Exception:
        pass
    try:
        from core import pool_disk_store

        out["pool_on_disk"] = bool(pool_disk_store.enabled())
        mapped = pool_disk_store.mapped_bytes()
        if mapped is not None:
            out["pool_mapped"] = int(mapped)
    except Exception:
        pass
    return out


def memory_snapshot(context: Any = None) -> dict[str, Any]:
    """JSON-ready memory figures in bytes; absent keys are unknown here."""
    if sys.platform.startswith("linux"):
        data: dict[str, Any] = {**_linux_process(), **_linux_system(), **_cgroup_limits()}
    elif os.name == "nt":
        data = _windows_process()
    else:
        data = _generic_process()
    data.update(_pool_info(context))
    data["platform"] = sys.platform
    return data


__all__ = ["memory_snapshot"]
