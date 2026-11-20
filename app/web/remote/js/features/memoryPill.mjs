// Toolbar memory indicator. Polls /api/system/memory and shows the backend's
// heap — the memory the kernel cannot drop, i.e. what runs a container into
// its limit. Total RSS is only in the tooltip: large search pools are served
// from memory-mapped files, so RSS also counts page cache the kernel evicts
// at will and would overstate usage by the size of the pool.
//
// Polling pauses while the tab is hidden and stops for good if the route is
// missing (an older backend), so the pill never shows a stale or fake number.

const POLL_MS = 5000;
const WARN_RATIO = 0.8;
const DANGER_RATIO = 0.92;

function fmtBytes(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return '—';
  const gib = n / 1024 ** 3;
  if (gib >= 10) return `${gib.toFixed(0)}G`;
  if (gib >= 1) return `${gib.toFixed(1)}G`;
  return `${Math.round(n / 1024 ** 2)}M`;
}

function fmtLong(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return '—';
  const gib = n / 1024 ** 3;
  return gib >= 1 ? `${gib.toFixed(2)} GiB` : `${Math.round(n / 1024 ** 2)} MiB`;
}

export function createMemoryPill({document, fetchImpl}) {
  const pill = document.getElementById('memPill');
  const value = document.getElementById('memValue');
  const doFetch = fetchImpl || ((...args) => globalThis.fetch(...args));
  let timer = null;
  let stopped = false;
  let inFlight = false;

  function render(m) {
    // Headline: heap where the platform reports it, else RSS, else peak.
    const used = m.heap ?? m.rss ?? m.peak_rss;
    if (used == null) {
      pill.classList.add('hidden');
      return;
    }
    pill.classList.remove('hidden');
    // The limit that matters is the container's; a bare host shows its RAM.
    const limit = m.limit ?? m.system_total ?? null;
    value.textContent = limit ? `${fmtBytes(used)} / ${fmtBytes(limit)}` : fmtBytes(used);
    const ratio = limit ? used / limit : 0;
    pill.classList.toggle('warn', ratio >= WARN_RATIO && ratio < DANGER_RATIO);
    pill.classList.toggle('danger', ratio >= DANGER_RATIO);

    const lines = ['NAIA 백엔드 메모리'];
    if (m.heap != null) lines.push(`힙(실사용, 회수 불가): ${fmtLong(m.heap)}`);
    if (m.file != null) lines.push(`디스크 맵 캐시(커널이 회수 가능): ${fmtLong(m.file)}`);
    if (m.rss != null) lines.push(`RSS 합계: ${fmtLong(m.rss)}`);
    if (m.peak_rss != null) lines.push(`RSS 최고치: ${fmtLong(m.peak_rss)}`);
    if (m.limit != null) {
      lines.push(`컨테이너 한도: ${fmtLong(m.limit)}`
        + (m.limit_usage != null ? ` (캐시 포함 사용 ${fmtLong(m.limit_usage)})` : ''));
    }
    if (m.system_total != null) {
      lines.push(`시스템: ${fmtLong(m.system_total)}`
        + (m.system_available != null ? ` 중 ${fmtLong(m.system_available)} 여유` : ''));
    }
    if (m.pool_rows != null) {
      let pool = `풀: ${Number(m.pool_rows).toLocaleString()}행`;
      if (m.pool_on_disk === false) pool += ' (RAM 모드)';
      else if (m.pool_mapped) pool += ` · 디스크 매핑 ${fmtLong(m.pool_mapped)}`;
      lines.push(pool);
    }
    // data-naia-guide, not title: the app's tooltip absorbs `title` as a
    // one-line tip, while guide tips keep line breaks (white-space: pre-line).
    pill.dataset.naiaGuide = lines.join('\n');
  }

  async function poll() {
    if (stopped || inFlight) return;
    if (document.visibilityState === 'hidden') return;
    inFlight = true;
    try {
      const response = await doFetch('/api/system/memory', {cache: 'no-store'});
      if (response.status === 404) {
        stop();
        pill.classList.add('hidden');
        return;
      }
      if (!response.ok) return;
      render(await response.json());
    } catch (_error) {
      // Backend restarting or offline: keep the last reading, try again later.
    } finally {
      inFlight = false;
    }
  }

  function stop() {
    stopped = true;
    if (timer) clearInterval(timer);
    timer = null;
  }

  function init() {
    if (!pill || !value) return;
    poll();
    timer = setInterval(poll, POLL_MS);
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') poll();
    });
  }

  return {init, poll, stop, render};
}
