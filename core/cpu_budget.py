"""How many CPUs this process may actually use.

``os.cpu_count()`` reports the machine, not the share this process was given.
Inside a container that is almost always wrong: ``docker run --cpus=4`` on a
32-core host still reports 32, so a pool sized from it oversubscribes by 8x and
every worker runs slower than one would have. The real ceilings are the CFS
quota the container was started with and the CPU set it was pinned to, so take
the smallest of what all three sources allow.

Both cgroup layouts are read: v2 exposes ``cpu.max`` as ``"<quota> <period>"``
(``"max"`` meaning unlimited), v1 splits it into ``cpu.cfs_quota_us`` and
``cpu.cfs_period_us`` (``-1`` meaning unlimited).
"""

from __future__ import annotations

import os
from pathlib import Path

CGROUP_V2_MAX = Path("/sys/fs/cgroup/cpu.max")
CGROUP_V1_QUOTA = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
CGROUP_V1_PERIOD = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

# A quota can be fractional (``--cpus=1.5``). Round up: half a core is still a
# core's worth of work that can overlap with another thread's IO wait.
_MIN_CPUS = 1


def _read_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _quota_cpus() -> int | None:
    """Cores allowed by the CFS quota, or None when unlimited/unreadable."""
    try:
        raw = CGROUP_V2_MAX.read_text(encoding="utf-8").split()
    except OSError:
        raw = []
    if len(raw) == 2:
        if raw[0] == "max":
            return None
        try:
            quota, period = int(raw[0]), int(raw[1])
        except ValueError:
            return None
        if quota > 0 and period > 0:
            return max(_MIN_CPUS, -(-quota // period))
        return None

    quota = _read_int(CGROUP_V1_QUOTA)
    period = _read_int(CGROUP_V1_PERIOD)
    if quota is not None and quota > 0 and period is not None and period > 0:
        return max(_MIN_CPUS, -(-quota // period))
    return None


def _affinity_cpus() -> int | None:
    """Cores in this process's CPU set (``--cpuset-cpus``, taskset), if knowable."""
    try:
        return len(os.sched_getaffinity(0)) or None
    except (AttributeError, OSError):
        return None


def available_cpus() -> int:
    """Usable core count: the tightest of quota, affinity and machine size."""
    limits = [value for value in (_quota_cpus(), _affinity_cpus(), os.cpu_count()) if value]
    return max(_MIN_CPUS, min(limits)) if limits else _MIN_CPUS


def pool_workers(work_items: int, *, cap: int) -> int:
    """Worker count for ``work_items`` units of parallel work.

    Never more workers than there is work, never more than ``cap`` (which is
    where a caller expresses its own memory or IO ceiling), never fewer than one.
    """
    if work_items <= 1:
        return 1
    return max(1, min(int(work_items), int(cap), available_cpus()))


__all__ = ["available_cpus", "pool_workers"]
