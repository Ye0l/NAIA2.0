"""이미지 API 전송 — NAIA Custom Extension (naia_ext_api=1).

생성이 끝난 이미지를 임의의 HTTP 엔드포인트로 밀어 넣는다. 아카이빙 서버, 디스코드
웹훅 프록시, 업스케일 파이프라인, 사내 갤러리 등 "결과를 다른 데로도 보내고 싶다"는
용도 전반을 덮는다.

- **형식**: WebP(무손실 토글) / PNG / JPEG / 원본(저장된 파일 바이트 그대로).
- **메타데이터**: 포함 여부를 끄고 켤 수 있다. 켜면 프롬프트·시드 등 NAI tEXt 청크를
  (1) 요청 본문의 ``metadata`` 필드로 보내고, (2) 별도 토글로 이미지 파일 자체에도
  심는다(PNG=tEXt, WebP/JPEG=EXIF). 끄면 픽셀만 나간다.
- **사용자 정의 값**: ``key=value`` 행을 원하는 만큼 추가해 폼/JSON 본문에 실을 수
  있고, 헤더도 같은 방식으로 붙인다(``Authorization: Bearer ...`` 등).
  값에는 ``{request_id}`` ``{seed}`` ``{date}`` 같은 자리표시자를 쓸 수 있다.

노출 계약대로 **엔드포인트·헤더·타임아웃 같은 배선 설정은 Settings ▸ Extension**
(scope global)에, **형식·메타데이터·사용자 정의 값 같은 동작 설정은 퀵 버튼 팝업**
(scope module)에 나뉘어 있다.

전송은 퀵 팝업의 **Activate This Script**가 켜져 있을 때만 일어난다 — 호스트가 작동
OFF인 확장의 이벤트 콜백을 아예 호출하지 않으므로, 구성해 두고 필요할 때만 켜는
운용이 가능하다. **엔드포인트 테스트** 버튼은 작동 OFF에서도 눌린다.

의존성 없음 — requests/Pillow/piexif 모두 본체가 이미 가진 것만 쓴다.

설치: 이 폴더를 user-data의 ``extensions/`` 아래로 복사 → Settings ▸ Extension에서
활성화 → 엔드포인트 URL 입력.
"""

import base64
import json
import queue
import re
import threading
import time
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

FORMAT_WEBP = "webp"
FORMAT_PNG = "png"
FORMAT_JPEG = "jpeg"
FORMAT_ORIGINAL = "original"

MODE_MULTIPART = "multipart"
MODE_JSON = "json"

# 한 번에 들고 있을 전송 대기 수. 생성이 전송보다 빠를 때 메모리가 무한정 늘지
# 않도록 막는다 — 넘치면 가장 오래된 것을 버리지 않고 새 것을 거절한다(로그+토스트).
MAX_PENDING = 32
# 원본 모드에서 자동 저장이 끝나기를 기다리는 한계. 저장은 결과 이벤트 뒤 비동기라
# file_path 가 잠시 빈 문자열일 수 있다(get_result_image 계약).
ORIGINAL_WAIT_SECONDS = 5.0
ORIGINAL_POLL_INTERVAL = 0.25
# 전송 워커가 이만큼 놀면 스레드를 놓아 준다(다음 결과에 다시 뜬다).
WORKER_IDLE_SECONDS = 30.0
# 응답 본문을 로그에 남길 때의 상한.
RESPONSE_LOG_CHARS = 300

# NAIA/NovelAI 가 PNG tEXt 로 남기는 키(core/api_service.py 의 보존 목록과 동일).
# parameters 는 WEBUI/ComfyUI 계열이 쓰는 관례적 키라 함께 걷는다.
METADATA_KEYS = (
    "Title",
    "Description",
    "Software",
    "Source",
    "Comment",
    "Generation time",
    "Author",
    "parameters",
)

PLACEHOLDER_RE = re.compile(r"\{([a-z_][a-z0-9_]*)\}")

DEFAULT_SETTINGS = {
    # ── 배선 (Settings ▸ Extension) ─────────────────────────────
    "endpoint_url": "",
    "custom_headers": [],
    "timeout": 30,
    "retries": 2,
    "verify_tls": True,
    # ── 동작 (퀵 버튼 팝업) ─────────────────────────────────────
    "payload_mode": MODE_MULTIPART,
    "file_field": "file",
    "image_format": FORMAT_WEBP,
    "webp_lossless": False,
    "quality": 90,
    "include_metadata": True,
    "metadata_in_file": True,
    "custom_fields": [],
    "filename_template": "{request_id}",
}


# ── 인코딩 ───────────────────────────────────────────────────────

def _collect_metadata(image):
    """PIL 사본의 info 에 남아 있는 생성 메타데이터를 텍스트 맵으로 걷는다."""
    info = getattr(image, "info", None) or {}
    out = {}
    for key in METADATA_KEYS:
        value = info.get(key)
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, str) and value:
            out[key] = value
    return out


def _comment_json(metadata):
    """NAI 의 Comment 는 prompt/seed/steps 가 든 JSON 문자열이다. 파싱되면 함께 싣는다."""
    raw = metadata.get("Comment")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _without_info(image):
    """메타데이터를 뗀 사본. copy() 는 info 까지 들고 오므로 명시적으로 비운다."""
    clean = image.copy()
    try:
        clean.info = {}
    except (AttributeError, TypeError):
        pass
    return clean


def _exif_bytes(metadata, log):
    """메타데이터 맵을 EXIF 블록으로. WebP/JPEG 는 tEXt 청크가 없어 EXIF 로 옮긴다."""
    description = metadata.get("Description") or ""
    software = metadata.get("Software") or "NAIA"
    try:
        payload = json.dumps(metadata, ensure_ascii=False)
    except (TypeError, ValueError):
        payload = str(metadata)
    try:
        import piexif
        import piexif.helper

        exif_dict = {
            "0th": {
                piexif.ImageIFD.ImageDescription: description.encode("utf-8", "replace"),
                piexif.ImageIFD.Software: software.encode("utf-8", "replace"),
            },
            "Exif": {
                piexif.ExifIFD.UserComment: piexif.helper.UserComment.dump(
                    payload, encoding="unicode"
                ),
            },
        }
        return piexif.dump(exif_dict)
    except Exception as exc:
        # piexif 가 없거나 덤프가 실패해도 전송 자체를 막지는 않는다 — Pillow 의
        # 기본 Exif 로 ImageDescription 만이라도 싣고 넘어간다.
        try:
            from PIL import Image

            exif = Image.Exif()
            exif[0x010E] = description or payload
            exif[0x0131] = software
            return exif.tobytes()
        except Exception:
            log(f"EXIF 구성 실패 — 메타데이터를 파일에 심지 못했습니다: {exc}")
            return b""


def _read_original(file_path, wait_seconds, log):
    """자동 저장된 파일을 그대로 읽는다. 저장이 비동기라 잠깐 기다려 준다."""
    deadline = time.monotonic() + max(0.0, wait_seconds)
    path = Path(file_path) if file_path else None
    while True:
        if path is not None and path.is_file():
            try:
                data = path.read_bytes()
                if data:
                    return data, path.suffix.lower().lstrip(".") or "png"
            except OSError as exc:
                log(f"원본 파일 읽기 실패({path.name}): {exc}")
                return None, ""
        if time.monotonic() >= deadline:
            return None, ""
        time.sleep(ORIGINAL_POLL_INTERVAL)


def _encode_image(image, metadata, settings, file_path, log):
    """(bytes, 확장자, content_type) 또는 실패 시 (None, "", "")."""
    fmt = str(settings.get("image_format") or FORMAT_WEBP).lower()
    embed = bool(settings.get("include_metadata")) and bool(settings.get("metadata_in_file"))

    if fmt == FORMAT_ORIGINAL:
        data, ext = _read_original(file_path, ORIGINAL_WAIT_SECONDS, log)
        if data is not None:
            return data, ext, _content_type(ext)
        log("원본 파일을 찾지 못해 PNG 로 인코딩합니다 (자동 저장이 꺼져 있거나 지연됨).")
        fmt = FORMAT_PNG

    if fmt == FORMAT_WEBP and not _webp_available():
        log("이 Pillow 빌드에 WebP 지원이 없어 PNG 로 대체합니다.")
        fmt = FORMAT_PNG

    source = image if embed else _without_info(image)
    buffer = BytesIO()

    if fmt == FORMAT_PNG:
        pnginfo = None
        if embed and metadata:
            try:
                from PIL.PngImagePlugin import PngInfo

                pnginfo = PngInfo()
                for key, value in metadata.items():
                    pnginfo.add_text(key, value)
            except Exception as exc:
                log(f"PNG 메타데이터 구성 실패 — 픽셀만 보냅니다: {exc}")
                pnginfo = None
        source.save(buffer, format="PNG", pnginfo=pnginfo)
        return buffer.getvalue(), "png", "image/png"

    exif = _exif_bytes(metadata, log) if (embed and metadata) else b""

    if fmt == FORMAT_JPEG:
        flat = source if source.mode in ("RGB", "L") else source.convert("RGB")
        flat.save(
            buffer,
            format="JPEG",
            quality=_clamp_int(settings.get("quality"), 1, 100, 90),
            exif=exif,
        )
        return buffer.getvalue(), "jpg", "image/jpeg"

    options = {"quality": _clamp_int(settings.get("quality"), 1, 100, 90)}
    if settings.get("webp_lossless"):
        options["lossless"] = True
    if exif:
        options["exif"] = exif
    source.save(buffer, format="WEBP", **options)
    return buffer.getvalue(), "webp", "image/webp"


def _webp_available():
    try:
        from PIL import features

        return bool(features.check("webp"))
    except Exception:
        return False


def _content_type(ext):
    return {
        "png": "image/png",
        "webp": "image/webp",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "tif": "image/tiff",
        "tiff": "image/tiff",
    }.get(str(ext).lower(), "application/octet-stream")


def _clamp_int(value, low, high, fallback):
    try:
        return max(low, min(high, int(value)))
    except (TypeError, ValueError):
        return fallback


# ── 사용자 정의 값 ───────────────────────────────────────────────

def _expand(text, values):
    """``{request_id}`` 류 자리표시자만 치환한다. 모르는 이름은 그대로 둔다
    (str.format 과 달리 사용자가 적은 중괄호에 KeyError 로 죽지 않는다)."""
    def replace(match):
        name = match.group(1)
        if name in values:
            return str(values[name])
        return match.group(0)

    return PLACEHOLDER_RE.sub(replace, str(text or ""))


def _split_pairs(rows, separator, values, log, what):
    """``key=value`` / ``Name: value`` 행 목록을 dict 로. 잘못된 행은 건너뛰고 알린다."""
    out = {}
    for row in rows or []:
        line = str(row or "").strip()
        if not line or line.startswith("#"):
            continue
        if separator not in line:
            log(f"{what} 행을 건너뜁니다 (‘{separator}’ 없음): {line[:60]}")
            continue
        key, _, value = line.partition(separator)
        key = key.strip()
        if not key:
            log(f"{what} 행을 건너뜁니다 (이름 없음): {line[:60]}")
            continue
        out[key] = _expand(value.strip(), values)
    return out


def _placeholder_values(info, metadata, ext):
    comment = _comment_json(metadata) or {}
    now = time.localtime()
    return {
        "request_id": str(info.get("request_id") or ""),
        "prompt_run_id": str(info.get("prompt_run_id") or ""),
        "api_mode": str(info.get("api_mode") or ""),
        "ext_origin": str(info.get("ext_origin") or ""),
        "ext": str(ext or ""),
        "seed": str(comment.get("seed", "")),
        "steps": str(comment.get("steps", "")),
        "scale": str(comment.get("scale", "")),
        "sampler": str(comment.get("sampler", "")),
        "prompt": metadata.get("Description", ""),
        "software": metadata.get("Software", ""),
        "timestamp": str(int(time.time())),
        "date": time.strftime("%Y%m%d", now),
        "time": time.strftime("%H%M%S", now),
    }


def _safe_filename(name, ext):
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "_", str(name or "").strip()).strip("._-")
    cleaned = cleaned[:120] or f"image_{int(time.time())}"
    return f"{cleaned}.{ext}" if ext else cleaned


LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1", "::ffff:127.0.0.1"}


def _in_container():
    """컨테이너 안에서 도는가. localhost 의 의미가 달라지는 유일한 경우라 판별한다."""
    try:
        if Path("/.dockerenv").exists():
            return True
    except OSError:
        pass
    try:
        cgroup = Path("/proc/self/cgroup").read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return any(marker in cgroup for marker in ("docker", "containerd", "kubepods"))


def _endpoint_hint(url):
    """실패 메시지에 덧붙일 한 줄 진단. 짚을 게 없으면 빈 문자열."""
    try:
        host = (urlparse(str(url or "")).hostname or "").lower()
    except (ValueError, AttributeError):
        return ""
    if host in LOOPBACK_HOSTS and _in_container():
        # 컨테이너 안의 localhost 는 호스트가 아니라 컨테이너 자신이다. 도커에서
        # 가장 흔하게 밟는 함정이라 403/404/연결거부를 이걸로 오해하기 쉽다.
        return ("힌트: 컨테이너 안에서 localhost 는 호스트가 아니라 컨테이너 자신입니다. "
                "호스트에서 도는 서비스라면 host.docker.internal 을, 다른 컨테이너면 "
                "그 서비스 이름을 쓰세요.")
    return ""


# ── 확장 본체 ────────────────────────────────────────────────────

class ApiForwarder:
    def __init__(self, ctx):
        self.ctx = ctx
        self._queue = queue.Queue(maxsize=MAX_PENDING)
        self._worker = None
        self._worker_lock = threading.Lock()
        self._last_ok = None  # 성공/실패가 뒤집힐 때만 토스트를 띄우기 위한 상태

    # -- 알림 -----------------------------------------------------
    def _log(self, message):
        try:
            self.ctx.log(message)
        except Exception:
            pass

    def _toast(self, message, level="error"):
        try:
            self.ctx.show_toast(message, level)
        except Exception:
            self._log(message)

    def _report(self, ok, message):
        """상태가 바뀔 때만 토스트 — 매 장 성공 토스트로 화면을 덮지 않는다."""
        self._log(message)
        if ok != self._last_ok:
            self._toast(message, "success" if ok else "error")
        self._last_ok = ok

    # -- 이벤트 ---------------------------------------------------
    def on_generation_result(self, info):
        if not isinstance(info, dict):
            return
        request_id = str(info.get("request_id") or "")
        if not request_id:
            return
        settings = self.ctx.load_settings(DEFAULT_SETTINGS)
        if not str(settings.get("endpoint_url") or "").strip():
            self._toast(
                "이미지 API 전송: 엔드포인트 URL이 비어 있습니다 — Settings ▸ Extension 에서 지정하세요.",
                "warning",
            )
            return

        fetched = self.ctx.get_result_image(request_id)
        if not fetched.get("ok"):
            self._log(f"이미지 회수 실패({request_id[:8]}): {fetched.get('message')}")
            return

        job = {
            "info": dict(info),
            "image": fetched["image"],
            "file_path": fetched.get("file_path") or "",
            "settings": settings,
        }
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._toast(
                f"이미지 API 전송: 대기열이 가득 차 {request_id[:8]} 을 건너뜁니다 "
                f"(전송이 생성 속도를 못 따라갑니다).",
                "warning",
            )
            return
        self._ensure_worker()

    # -- 워커 -----------------------------------------------------
    def _ensure_worker(self):
        """전송은 네트워크 대기가 있으므로 전용 스레드 하나에서 직렬 처리한다.
        콜백 스레드를 붙잡지 않고, 장당 스레드를 새로 만들지도 않는다."""
        with self._worker_lock:
            if self._worker is not None:
                return
            self._worker = threading.Thread(
                target=self._drain, name="api-forwarder", daemon=True
            )
            self._worker.start()

    def _drain(self):
        while True:
            try:
                job = self._queue.get(timeout=WORKER_IDLE_SECONDS)
            except queue.Empty:
                # 놀고 있으면 스레드를 놓아 준다. 생산자는 put 다음에
                # _ensure_worker 를 부르므로, 같은 락 아래에서 큐를 한 번 더 보고
                # 나가야 "막 넣은 작업을 아무도 안 집어가는" 경합이 안 생긴다.
                with self._worker_lock:
                    if self._queue.empty():
                        self._worker = None
                        return
                continue
            try:
                self._deliver(job)
            except Exception as exc:
                self._report(False, f"이미지 API 전송 실패: {exc}")
            finally:
                self._queue.task_done()

    def _deliver(self, job):
        info = job["info"]
        settings = job["settings"]
        metadata = _collect_metadata(job["image"])

        data, ext, content_type = _encode_image(
            job["image"], metadata, settings, job["file_path"], self._log
        )
        if not data:
            self._report(False, "이미지 API 전송: 인코딩 결과가 비었습니다.")
            return

        values = _placeholder_values(info, metadata, ext)
        filename = _safe_filename(
            _expand(settings.get("filename_template") or "{request_id}", values), ext
        )
        headers = _split_pairs(
            settings.get("custom_headers"), ":", values, self._log, "헤더"
        )
        fields = _split_pairs(
            settings.get("custom_fields"), "=", values, self._log, "사용자 정의 필드"
        )

        include_metadata = bool(settings.get("include_metadata"))
        payload_metadata = None
        if include_metadata:
            payload_metadata = dict(metadata)
            parsed = _comment_json(metadata)
            if parsed is not None:
                payload_metadata["comment_json"] = parsed

        base = {
            "request_id": values["request_id"],
            "prompt_run_id": values["prompt_run_id"],
            "api_mode": values["api_mode"],
        }
        ok, message = self._post(
            settings, headers, base, fields, payload_metadata,
            filename, content_type, data,
        )
        size_kb = len(data) / 1024
        url = str(settings.get("endpoint_url") or "").strip()
        if ok:
            self._report(True, f"이미지 API 전송 완료: {filename} ({size_kb:.0f}KB) — {message}")
        else:
            # 어디로 무엇을 보내다 실패했는지까지 남긴다 — 응답 코드만으로는
            # 받는 쪽이 거절한 건지 엉뚱한 곳을 겨눈 건지 구분이 안 된다.
            hint = _endpoint_hint(url)
            detail = f"이미지 API 전송 실패: {url} — {message}"
            self._report(False, f"{detail} / {hint}" if hint else detail)

    def _post(self, settings, headers, base, fields, metadata,
              filename, content_type, data):
        """재시도까지 포함한 1건 전송 → (성공여부, 사람이 읽을 메시지)."""
        try:
            import requests
        except ImportError:
            return False, "requests 를 불러올 수 없습니다 (본체 의존성 누락)."

        url = str(settings.get("endpoint_url") or "").strip()
        timeout = _clamp_int(settings.get("timeout"), 1, 600, 30)
        attempts = _clamp_int(settings.get("retries"), 0, 5, 2) + 1
        verify = bool(settings.get("verify_tls", True))
        mode = str(settings.get("payload_mode") or MODE_MULTIPART).lower()

        last = "알 수 없는 오류"
        for attempt in range(1, attempts + 1):
            try:
                if mode == MODE_JSON:
                    body = dict(base)
                    body.update(fields)
                    body["filename"] = filename
                    body["content_type"] = content_type
                    body["image_base64"] = base64.b64encode(data).decode("ascii")
                    if metadata is not None:
                        body["metadata"] = metadata
                    response = requests.post(
                        url, json=body, headers=headers, timeout=timeout, verify=verify
                    )
                else:
                    form = dict(base)
                    form.update(fields)
                    if metadata is not None:
                        form["metadata"] = json.dumps(metadata, ensure_ascii=False)
                    field_name = str(settings.get("file_field") or "file").strip() or "file"
                    response = requests.post(
                        url,
                        data=form,
                        files={field_name: (filename, data, content_type)},
                        headers=headers,
                        timeout=timeout,
                        verify=verify,
                    )
            except Exception as exc:
                # 네트워크/TLS 오류는 재시도 대상. 예외 문자열에 헤더는 담기지 않는다.
                last = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code < 300:
                    return True, f"HTTP {response.status_code}"
                snippet = (response.text or "")[:RESPONSE_LOG_CHARS].replace("\n", " ")
                last = f"HTTP {response.status_code} {snippet}".strip()
                # 4xx 는 다시 보내도 같은 답이다 — 429(속도 제한)만 예외.
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    return False, last
            if attempt < attempts:
                time.sleep(min(8.0, 2.0 ** (attempt - 1)))
        return False, last

    # -- 패널 액션 -------------------------------------------------
    def on_action(self, key):
        if key != "test_send":
            return
        settings = self.ctx.load_settings(DEFAULT_SETTINGS)
        if not str(settings.get("endpoint_url") or "").strip():
            self._toast("엔드포인트 URL을 먼저 지정하세요 (Settings ▸ Extension).", "warning")
            return
        self._toast("테스트 이미지를 전송합니다…", "info")
        threading.Thread(
            target=self._run_test, args=(settings,), name="api-forwarder-test", daemon=True
        ).start()

    def _run_test(self, settings):
        try:
            from PIL import Image

            image = Image.new("RGB", (64, 64), (32, 96, 160))
            image.info = {
                "Software": "NAIA",
                "Description": "api_forwarder test image",
                "Comment": json.dumps({"prompt": "api_forwarder test image", "seed": 0}),
            }
            job = {
                "info": {
                    "request_id": f"test-{int(time.time())}",
                    "prompt_run_id": "",
                    "api_mode": self._api_mode(),
                },
                "image": image,
                # 테스트는 실제 저장 파일이 없으므로 원본 모드여도 인코딩으로 떨어진다.
                "file_path": "",
                "settings": settings,
            }
            self._deliver(job)
        except Exception as exc:
            self._toast(f"테스트 전송 실패: {exc}", "error")

    def _api_mode(self):
        try:
            return str(self.ctx.get_api_mode() or "")
        except Exception:
            return ""


# ── 패널 ─────────────────────────────────────────────────────────

def _register_panel(ctx, ext):
    if not hasattr(ctx, "register_panel"):
        return
    when_multipart = {"field": "payload_mode", "in": [MODE_MULTIPART]}
    when_lossy = {"field": "image_format", "in": [FORMAT_WEBP, FORMAT_JPEG]}
    when_webp = {"field": "image_format", "in": [FORMAT_WEBP]}
    # 렌더러는 현재 설정값을 String() 으로 눕혀 비교한다 — bool 은 "true"/"false".
    when_metadata = {"field": "include_metadata", "in": ["true"]}

    ctx.register_panel(
        fields=[
            # ── 배선: Settings ▸ Extension (scope global) ───────────
            {"key": "endpoint_url", "type": "text", "scope": "global",
             "default": "", "label": "엔드포인트 URL", "section": "전송 대상", "order": 0,
             "placeholder": "https://example.com/api/images",
             "help": "이미지를 POST 할 주소. 비어 있으면 전송하지 않습니다"},
            {"key": "custom_headers", "type": "list", "scope": "global",
             "default": [], "label": "요청 헤더", "section": "전송 대상", "order": 1,
             "placeholder": "Authorization: Bearer xxxxx",
             "help": "‘이름: 값’ 한 줄에 하나. 인증 토큰도 여기에 넣습니다 "
                     "(값에 {date} 같은 자리표시자 사용 가능)"},
            {"key": "timeout", "type": "int", "scope": "global",
             "min": 1, "max": 600, "default": 30, "label": "타임아웃(초)",
             "section": "전송 대상", "order": 2},
            {"key": "retries", "type": "int", "scope": "global",
             "min": 0, "max": 5, "default": 2, "label": "재시도 횟수",
             "section": "전송 대상", "order": 3,
             "help": "네트워크 오류·5xx·429 에만 재시도합니다(지수 백오프). "
                     "그 밖의 4xx 는 다시 보내도 같은 답이라 즉시 포기"},
            {"key": "verify_tls", "type": "bool", "scope": "global",
             "default": True, "label": "TLS 인증서 검증", "section": "전송 대상", "order": 4,
             "help": "끄면 https 상대를 확인하지 않습니다. 사설 인증서를 쓰는 "
                     "내부망 엔드포인트가 아니라면 켜 두세요"},

            # ── 동작: 퀵 버튼 팝업 (scope module) ───────────────────
            {"key": "image_format", "type": "select",
             "options": [FORMAT_WEBP, FORMAT_PNG, FORMAT_JPEG, FORMAT_ORIGINAL],
             "default": FORMAT_WEBP, "label": "전송 형식", "section": "이미지", "order": 10,
             "help": "original = 자동 저장된 파일을 바이트 그대로 전송(자동 저장이 "
                     "켜져 있어야 하며, 저장 전이면 PNG 로 대체)"},
            {"key": "webp_lossless", "type": "bool", "default": False,
             "label": "WebP 무손실", "section": "이미지", "order": 11,
             "visible_when": when_webp,
             "help": "켜면 품질 대신 무손실로 인코딩합니다 (파일이 커집니다)"},
            {"key": "quality", "type": "int", "min": 1, "max": 100, "default": 90,
             "label": "품질", "section": "이미지", "order": 12,
             "visible_when": when_lossy,
             "help": "WebP·JPEG 손실 압축 품질"},

            {"key": "include_metadata", "type": "bool", "default": True,
             "label": "메타데이터 포함", "section": "메타데이터", "order": 20,
             "help": "프롬프트·시드 등 생성 정보를 요청 본문의 metadata 필드로 "
                     "함께 보냅니다. 끄면 픽셀만 나갑니다"},
            {"key": "metadata_in_file", "type": "bool", "default": True,
             "label": "이미지 파일에도 심기", "section": "메타데이터", "order": 21,
             "visible_when": when_metadata,
             "help": "PNG=tEXt 청크, WebP·JPEG=EXIF 로 이미지 안에 함께 기록합니다. "
                     "끄면 본문 필드로만 보내고 파일은 깨끗하게 유지"},

            {"key": "payload_mode", "type": "select",
             "options": [MODE_MULTIPART, MODE_JSON],
             "default": MODE_MULTIPART, "label": "요청 형식",
             "column": "right", "section": "요청", "order": 30,
             "help": "multipart = 파일 업로드(form-data) · json = base64 를 담은 JSON 본문"},
            {"key": "file_field", "type": "text", "default": "file",
             "label": "파일 필드 이름", "column": "right", "section": "요청", "order": 31,
             "visible_when": when_multipart, "placeholder": "file",
             "help": "multipart 파트 이름. 받는 쪽 API 가 기대하는 이름으로 맞추세요"},
            {"key": "filename_template", "type": "text", "default": "{request_id}",
             "label": "파일 이름", "column": "right", "section": "요청", "order": 32,
             "placeholder": "{date}_{seed}",
             "help": "확장자는 전송 형식에 따라 자동으로 붙습니다"},
            {"key": "custom_fields", "type": "list", "default": [],
             "label": "사용자 정의 값", "column": "right", "section": "요청", "order": 33,
             "placeholder": "album=naia",
             "help": "‘이름=값’ 한 줄에 하나. 본문에 그대로 실립니다. "
                     "쓸 수 있는 자리표시자: {request_id} {prompt_run_id} {api_mode} "
                     "{seed} {steps} {scale} {sampler} {prompt} {software} "
                     "{ext} {timestamp} {date} {time}"},
            {"key": "test_send", "type": "action", "label": "▶ 엔드포인트 테스트",
             "column": "right", "section": "요청", "order": 34,
             "help": "지금 설정으로 64×64 테스트 이미지를 한 장 보냅니다 "
                     "(작동 OFF 상태에서도 눌립니다)"},
        ],
        title="이미지 API 전송",
        on_action=ext.on_action,
    )


def register(ctx):
    ext = ApiForwarder(ctx)
    ctx.subscribe("generation_result_available", ext.on_generation_result)
    _register_panel(ctx, ext)
    ctx.log("ready — 이미지 API 전송 (엔드포인트는 Settings ▸ Extension, 전송 설정은 퀵 버튼 팝업)")
