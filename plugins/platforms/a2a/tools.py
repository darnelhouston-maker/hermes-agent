"""A2A client tools (``a2a`` toolset): a2a_discover/call/list/history/orchestrate talk to *other*
agents. Peers come from config.yaml ``a2a_agents: {name: {url, auth: {type: bearer, token}, timeout,
capabilities}}``. Stdlib urllib; wire format is A2A v1.0 ``SendMessage`` (v0.3 replies still parse)."""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from gateway.platforms._shared import coerce_port as _coerce_int

from . import controller, protocol, security, work_items

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 120
_POLL_INTERVAL = 2.0
_POLL_HTTP_TIMEOUT = 30
_ORCHESTRATE_MAX_WORKERS = 6  # max parallel peers for fan-out
_TRACE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="a2a-telegram-trace")
_CANONICAL_PEER_LABELS = {
    "jeff": "Jeff",
    "mac2": "Mac 2",
    "medical_records": "Medical Records",
    "nova": "Nova",
    "steve": "Steve - Yadira's Accountant + Web Builder",
    "remy-windows-dev": "Remy Windows Dev",
}
_TRACE_STATE_LABELS = {
    protocol.STATE_SUBMITTED: "submitted",
    protocol.STATE_WORKING: "working",
    protocol.STATE_INPUT_REQUIRED: "input required",
    protocol.STATE_AUTH_REQUIRED: "authentication required",
    protocol.STATE_COMPLETED: "completed",
    protocol.STATE_FAILED: "failed",
    protocol.STATE_CANCELED: "canceled",
    protocol.STATE_REJECTED: "rejected",
    "transport-error": "transport error",
    "peer-error": "peer error",
}

_CALL_TERMINAL_STATES = protocol.TERMINAL_STATES | {protocol.STATE_INPUT_REQUIRED}


class A2ASubmitTimeout(TimeoutError):
    """The initial request timed out before a durable task receipt arrived."""

    def __init__(self, agent: str, context_id: str):
        super().__init__(f"A2A submission to {agent} timed out before a task receipt")
        self.agent = agent
        self.context_id = context_id


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Fail closed on redirects so peer auth and payloads stay on the allowed origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _load_config() -> dict:
    """Read-only view of config.yaml; peers are only read, never mutated (cache-safe)."""
    from hermes_cli.config import load_config_readonly
    return load_config_readonly() or {}


def _configured_peers() -> dict:
    return _load_config().get("a2a_agents") or {}


def _peer_from_entry(entry: dict, **extra: Any) -> dict:
    return {"url": entry.get("url", ""), "auth": entry.get("auth", {}) or {},
            "timeout": int(entry.get("timeout", _DEFAULT_TIMEOUT)), **extra}


def _resolve_peer(agent: str) -> Optional[dict]:
    """Resolve one reviewed, configured fleet peer by its canonical key."""
    if agent not in _CANONICAL_PEER_LABELS:
        return None
    entry = _configured_peers().get(agent)
    if not isinstance(entry, dict):
        return None
    url = str(entry.get("url") or "").strip()
    try:
        _normalized_origin(url)
    except ValueError:
        return None
    return _peer_from_entry(
        entry,
        capabilities=entry.get("capabilities", []) or [],
        description=str(entry.get("description") or "").strip(),
        tenant=entry.get("tenant", ""),
    )


def _single_line(value: Any, limit: int) -> str:
    """Collapse whitespace and bound config-derived display text."""
    return " ".join(str(value or "").split())[:limit]


def _configured_peer_roster() -> list[str]:
    """Return neutral, factual peer descriptions for the tool schema."""
    peers = _configured_peers()
    lines: list[str] = []
    for name, label in _CANONICAL_PEER_LABELS.items():
        entry = peers.get(name)
        if not isinstance(entry, dict):
            continue
        description = _single_line(entry.get("description"), 240)
        capabilities = [
            _single_line(value, 60)
            for value in (entry.get("capabilities") or [])[:12]
            if _single_line(value, 60)
        ]
        factual = description or (", ".join(capabilities) if capabilities else "configured A2A peer")
        lines.append(f"- {label} ({name}): {factual}")
    known = (_load_config().get("a2a_known_agents") or {})
    if isinstance(known, dict):
        for name, entry in known.items():
            if not isinstance(entry, dict):
                continue
            description = _single_line(entry.get("description"), 240)
            status = _single_line(entry.get("status"), 40) or "not connected"
            lines.append(f"- {name} [{status}]: {description or 'known fleet agent'}")
    return lines[:8]


def _routing_expectations() -> list[str]:
    raw = ((_load_config().get("a2a") or {}).get("routing_expectations") or [])
    if not isinstance(raw, list):
        return []
    return [_single_line(value, 280) for value in raw[:4] if _single_line(value, 280)]


def _a2a_chaining_blocked(target: str = "") -> bool:
    """Block outbound network A2A from an A2A-originated agent turn, with one reviewed a2a relay rule (local patch,
    Remy 9/30/2026: "each A2A, they should accept requests from the A2A and respond automatically so that things
    move on without getting blocked"): Watson is the hub: a turn that arrived over A2A may pass the request on to any OTHER configured peer, never straight back to the peer that asked. Every other host keeps
    the full block, so a chain is at most spoke -> hub -> spoke (-> mac2) and can never loop back."""
    try:
        from gateway.session_context import get_session_env

        platform = (get_session_env("HERMES_SESSION_PLATFORM", "") or "").strip().lower()
        caller = (get_session_env("HERMES_SESSION_USER_ID", "") or get_session_env("HERMES_SESSION_USER_NAME", "")
                  or "").strip().lower()
    except Exception:
        return True
    if platform != "a2a":
        return False
    target = str(target or "").strip().lower()
    return not (bool(target) and bool(caller) and target != caller)


def _is_hospice_related(message: str) -> bool:
    return re.search(r"\bhospice\b", str(message or ""), re.IGNORECASE) is not None


def _id_set(value: Any) -> set[str]:
    items = value if isinstance(value, (list, tuple, set)) else str(value or "").split(",")
    return {str(item).strip() for item in items if str(item).strip()}


def _trace_settings() -> dict:
    raw = ((_load_config().get("a2a") or {}).get("telegram_trace") or {})
    if not isinstance(raw, dict):
        raw = {}
    enabled = str(raw.get("enabled", "false")).strip().lower() in ("1", "true", "yes", "on")
    max_round_trips, idle_timeout = controller.limits()
    return {
        "enabled": enabled,
        "max_round_trips": max_round_trips,
        "idle_timeout_seconds": idle_timeout,
        "owner_chat_ids": _id_set(raw.get("owner_chat_ids")),
        "owner_user_ids": _id_set(raw.get("owner_user_ids")),
    }


def _trace_route() -> Optional[dict]:
    """Owner-approved private log chat for A2A traces (Remy 10/3/2026: A2A conversations get their own thread)."""
    raw = ((_load_config().get("a2a") or {}).get("telegram_trace") or {})
    if not isinstance(raw, dict):
        return None
    chat = str(raw.get("route_chat_id") or "").strip()
    if not chat.lstrip("-").isdigit():
        return None
    thread = str(raw.get("route_thread_id") or "").strip()
    return {"chat_id": chat, "thread_id": thread if thread.isdigit() else "", "message_id": ""}


def _trace_target(*, allow_inbound: bool = False) -> Optional[dict]:
    """Return the configured owner/private-chat trace target or fail closed."""
    settings = _trace_settings()
    if not settings["enabled"]:
        return None
    route = _trace_route()
    if route:
        return route
    if allow_inbound:
        owner_targets = settings["owner_chat_ids"] & settings["owner_user_ids"]
        if len(owner_targets) != 1:
            return None
        return {"chat_id": next(iter(owner_targets)), "thread_id": "", "message_id": ""}
    try:
        from gateway.session_context import get_session_env

        platform = (get_session_env("HERMES_SESSION_PLATFORM", "") or "").strip().lower()
        chat_id = (get_session_env("HERMES_SESSION_CHAT_ID", "") or "").strip()
        user_id = (get_session_env("HERMES_SESSION_USER_ID", "") or "").strip()
        chat_type = (get_session_env("HERMES_SESSION_CHAT_TYPE", "") or "").strip().lower()
        thread_id = (get_session_env("HERMES_SESSION_THREAD_ID", "") or "").strip()
        message_id = (get_session_env("HERMES_SESSION_MESSAGE_ID", "") or "").strip()
    except Exception:
        return None
    if platform != "telegram" or not chat_id or not user_id:
        return None
    if chat_id not in settings["owner_chat_ids"] or user_id not in settings["owner_user_ids"]:
        return None
    if chat_id != user_id or (chat_type and chat_type not in {"private", "direct", "dm"}):
        return None
    return {
        "chat_id": chat_id,
        "thread_id": thread_id if thread_id.isdigit() else "",
        "message_id": message_id if message_id.isdigit() else "",
    }


def _send_telegram_trace_job(target: dict, content: str) -> None:
    """Send one silent trace message off the A2A request's critical path."""
    try:
        from agent.secret_scope import get_secret

        token = (get_secret("TELEGRAM_BOT_TOKEN", "") or "").strip()
        if not token:
            return
        payload: dict[str, str] = {
            "chat_id": target["chat_id"],
            "text": content,
            "disable_notification": "true",
        }
        if target.get("thread_id"):
            payload["message_thread_id"] = target["thread_id"]
        if target.get("message_id"):
            payload["reply_parameters"] = json.dumps(
                {"message_id": int(target["message_id"]), "allow_sending_without_reply": True},
                separators=(",", ":"),
            )
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=urllib.parse.urlencode(payload).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310 (Telegram Bot API)
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            logger.warning("A2A owner trace delivery was rejected by Telegram")
    except Exception:
        logger.warning("A2A owner trace delivery failed")


def _queue_telegram_trace(
    *,
    agent: str,
    context_id: str,
    round_number: int,
    direction: str,
    state: str = "",
    body: str = "",
    hub_direction: str = "outbound",
) -> bool:
    """Echo a live A2A turn into the allowlisted owner Telegram chat."""
    target = _trace_target(allow_inbound=(hub_direction == "inbound"))
    peer_label = _CANONICAL_PEER_LABELS.get(agent) if agent in _configured_peers() else None
    if target is None or peer_label is None:
        return False
    cap = _trace_settings()["max_round_trips"]
    visible_body = str(body or "")
    if len(visible_body) > 3000:
        visible_body = visible_body[:2999] + "..."
    if hub_direction == "inbound" and direction == "request":
        content = f"A2A -> Watson · from {peer_label}\nRound {round_number}/{cap} · Context {context_id}\n{peer_label}:\n{visible_body}"
    elif hub_direction == "inbound":
        result = _TRACE_STATE_LABELS.get(state, "unknown")
        content = f"A2A <- Watson · to {peer_label}\nRound {round_number}/{cap} · Context {context_id} · {result}\nWatson:\n{visible_body}"
    elif direction == "request":
        content = f"A2A -> {peer_label}\nRound {round_number}/{cap} · Context {context_id}\nWatson:\n{visible_body}"
    else:
        result = _TRACE_STATE_LABELS.get(state, "unknown")
        content = f"A2A <- {peer_label}\nRound {round_number}/{cap} · Context {context_id} · {result}\n{peer_label}:\n{visible_body}"
    try:
        _TRACE_EXECUTOR.submit(_send_telegram_trace_job, target, content)
        return True
    except RuntimeError:
        logger.warning("A2A owner trace could not be queued")
        return False


def _auth_header(auth: dict) -> dict:
    return {"Authorization": f"Bearer {auth['token']}"} if auth and auth.get("type") == "bearer" and auth.get("token") else {}


def _http_json(url: str, headers: dict, timeout: int, method: str, data: Optional[bytes] = None) -> dict:
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with _NO_REDIRECT_OPENER.open(req, timeout=timeout) as resp:  # noqa: S310 (configured peers)
        return json.loads(resp.read().decode("utf-8"))


def _http_get_json(url: str, headers: dict, timeout: int) -> dict:
    return _http_json(url, headers, timeout, "GET")


def _http_post_json(url: str, body: dict, headers: dict, timeout: int) -> dict:
    hdrs = {"Content-Type": "application/json", "A2A-Version": protocol.PROTOCOL_VERSION, **headers}
    return _http_json(url, hdrs, timeout, "POST", json.dumps(body).encode("utf-8"))


def _is_timeout_error(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    return isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, (TimeoutError, socket.timeout))


def _stable_message_id(idempotency_key: str) -> str:
    if not idempotency_key:
        return ""
    return hashlib.sha256(
        b"hermes-a2a-request-v1\0" + idempotency_key.encode("utf-8", errors="replace")
    ).hexdigest()


def _task_fields(payload: Any, fallback_context: str) -> tuple[str, str, str, str]:
    payload = protocol.unwrap_send_message_response(payload)
    if not isinstance(payload, dict):
        return str(payload), fallback_context, "", ""
    context_id = str(payload.get("contextId") or fallback_context).strip() or fallback_context
    state = str((payload.get("status") or {}).get("state") or "")
    task_id = str(payload.get("id") or "").strip()
    return _reply_text_from_result(payload), context_id, state, task_id


def _task_rpc(
    base_url: str,
    card: Optional[dict],
    headers: dict,
    method: str,
    params: dict,
    timeout: int,
) -> Any:
    body = {
        "jsonrpc": "2.0",
        "id": protocol.new_task_id(),
        "method": method,
        "params": params,
    }
    resp = _http_post_json(_rpc_url(base_url, card), body, headers, timeout)
    if "error" in resp:
        raise ValueError(resp["error"].get("message", resp["error"]))
    return resp.get("result", {})


def _fetch_card(base_url: str, headers: dict, timeout: int) -> dict:
    """GET the v1.0 agent-card.json; on 404 fall back to the v0.2 agent.json alias."""
    base = base_url.rstrip("/")
    try:
        return _http_get_json(base + "/.well-known/agent-card.json", headers, timeout)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    return _http_get_json(base + "/.well-known/agent.json", headers, timeout)


def _select_jsonrpc_interface(card: Optional[dict]) -> Optional[dict]:
    if isinstance(card, dict):
        for iface in card.get("supportedInterfaces", []) or []:
            if isinstance(iface, dict) and iface.get("protocolBinding") == "JSONRPC" and iface.get("url"):
                return iface
    return None


def _normalized_origin(url: str) -> tuple[str, str, int]:
    """Return a strict HTTP(S) origin, rejecting credentials and malformed URLs."""
    parsed = urllib.parse.urlsplit(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("peer URL must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("peer URL must not contain user information")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("peer URL has an invalid port") from exc
    return parsed.scheme.lower(), parsed.hostname.lower(), port or (443 if parsed.scheme.lower() == "https" else 80)


def _same_origin_url(base_url: str, candidate_url: str) -> Optional[str]:
    """Resolve a card URL and accept it only on the configured peer origin."""
    try:
        resolved = urllib.parse.urljoin(base_url.rstrip("/") + "/", str(candidate_url or "").strip())
        if _normalized_origin(resolved) != _normalized_origin(base_url):
            return None
        if urllib.parse.urlsplit(resolved).fragment:
            return None
        return resolved
    except ValueError:
        return None


def _rpc_url(base_url: str, card: Optional[dict]) -> str:
    """Use a card RPC URL only when it stays on the configured peer origin."""
    _normalized_origin(base_url)
    if iface := _select_jsonrpc_interface(card):
        if candidate := _same_origin_url(base_url, str(iface["url"])):
            return candidate
    if isinstance(card, dict) and isinstance(card.get("url"), str) and card["url"]:
        if candidate := _same_origin_url(base_url, card["url"]):
            return candidate
    return base_url.rstrip("/")


def _send_task(
    agent_label: str,
    peer: dict,
    message: str,
    context_id: str,
    request_id: str = "",
) -> tuple[str, str, str, str]:
    """Send one bounded round and queue an owner-visible live transcript."""
    if _a2a_chaining_blocked(agent_label):
        raise RuntimeError("A2A chaining is disabled for A2A-origin sessions.")
    ctx = context_id or protocol.new_context_id()
    round_number = controller.reserve_round(agent_label, ctx)
    _queue_telegram_trace(
        agent=agent_label,
        context_id=ctx,
        round_number=round_number,
        direction="request",
        body=message,
    )
    try:
        reply, reply_ctx, state, task_id = _send_task_uncontrolled(
            agent_label,
            peer,
            message,
            ctx,
            request_id=request_id,
        )
    except ValueError as exc:
        _queue_telegram_trace(
            agent=agent_label,
            context_id=ctx,
            round_number=round_number,
            direction="response",
            state="peer-error",
            body=str(exc),
        )
        raise
    except Exception as exc:
        _queue_telegram_trace(
            agent=agent_label,
            context_id=ctx,
            round_number=round_number,
            direction="response",
            state="transport-error",
            body=str(exc),
        )
        raise
    _queue_telegram_trace(
        agent=agent_label,
        context_id=ctx,
        round_number=round_number,
        direction="response",
        state=state,
        body=reply,
    )
    return reply, reply_ctx, state, task_id


def _send_task_uncontrolled(
    agent_label: str,
    peer: dict,
    message: str,
    context_id: str,
    request_id: str = "",
) -> tuple[str, str, str, str]:
    """Submit a detachable task, then poll it within the configured caller wait budget."""
    base_url = peer.get("url", "")
    headers = _auth_header(peer.get("auth", {}) or {})
    timeout = int(peer.get("timeout", _DEFAULT_TIMEOUT))
    try:
        card = _fetch_card(base_url, headers, min(timeout, 30))  # best-effort, to learn the rpc URL
    except Exception:
        card = None
    ctx = context_id or protocol.new_context_id()
    wire_message = message
    message_id = _stable_message_id(request_id)
    # v1.0: contextId lives inside the Message, not at the params top level.
    rpc_body = {"jsonrpc": "2.0", "id": protocol.new_task_id(), "method": "SendMessage",
                "params": {
                    "message": protocol.text_message(
                        protocol.ROLE_USER,
                        wire_message,
                        context_id=ctx,
                        message_id=message_id,
                    ),
                    "configuration": {"blocking": False},
                }}
    iface = _select_jsonrpc_interface(card)
    tenant = str(iface["tenant"]) if iface and iface.get("tenant") else str(peer.get("tenant") or "")
    if tenant:
        rpc_body["params"]["tenant"] = tenant
    security.audit("outbound", agent_label, rpc_body["id"], wire_message)
    protocol.persist_message(ctx, "user", wire_message, rpc_body["id"])
    protocol.metrics.outbound_total += 1
    deadline = time.monotonic() + timeout
    try:
        resp = _http_post_json(_rpc_url(base_url, card), rpc_body, headers, timeout)
    except Exception as exc:
        if _is_timeout_error(exc):
            raise A2ASubmitTimeout(agent_label, ctx) from exc
        raise
    if "error" in resp:
        raise ValueError(f"Peer '{agent_label}' returned an error: {resp['error'].get('message', resp['error'])}")
    reply, reply_ctx, state, task_id = _task_fields(resp.get("result", {}), ctx)
    if reply_ctx != ctx and not task_id:
        raise ValueError(f"Peer '{agent_label}' returned a mismatched conversation context.")
    while task_id and state not in _CALL_TERMINAL_STATES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(_POLL_INTERVAL, remaining))
        try:
            snapshot = _task_rpc(
                base_url,
                card,
                headers,
                "GetTask",
                {"id": task_id, **({"tenant": tenant} if tenant else {})},
                max(1, min(_POLL_HTTP_TIMEOUT, int(max(1, remaining)))),
            )
        except Exception:
            logger.warning("A2A task status poll failed for peer %s; retaining accepted receipt", agent_label)
            break
        polled_reply, polled_ctx, polled_state, polled_task_id = _task_fields(snapshot, reply_ctx)
        if polled_task_id and polled_task_id != task_id:
            raise ValueError(f"Peer '{agent_label}' returned a mismatched task id.")
        if polled_ctx != reply_ctx:
            raise ValueError(f"Peer '{agent_label}' returned a mismatched conversation context.")
        reply, state = polled_reply, polled_state
    if state in _CALL_TERMINAL_STATES:
        protocol.persist_message(reply_ctx, "agent", reply, task_id or rpc_body["id"])
    protocol.metrics.inbound_total += 1
    return reply, reply_ctx, state, task_id


def _reply_text_from_result(result: Any) -> str:
    result = protocol.unwrap_send_message_response(result)
    if not isinstance(result, dict):
        return str(result)
    # Artifacts first (final output), then status message (interim/clarify), else bare Message.
    for artifact in result.get("artifacts", []) or []:
        txt = protocol.extract_text(artifact)
        if txt:
            return txt
    return protocol.extract_text((result.get("status", {}) or {}).get("message") or result)


_AUTH_ERR = "Error: peer '{agent}' rejected auth (HTTP {code}). Check the configured token."
_HTTP_CALL_ERRORS = {401: _AUTH_ERR, 403: _AUTH_ERR, 429: "Error: peer '{agent}' rate limited us (HTTP 429). Retry later."}


def _work_item_reference_or_create(work_item_id: str) -> tuple[str, str]:
    """Create a missing shared reference once; malformed references still fail closed."""
    try:
        return work_items.reference_prefix(work_item_id), ""
    except work_items.WorkItemError as exc:
        if exc.kind != "not_found":
            raise
    idempotency_key = "a2a-create-" + hashlib.sha256(work_item_id.encode("utf-8")).hexdigest()[:32]
    try:
        work_items.append({
            "work_item_id": work_item_id,
            "status": "open",
            "note": "Created automatically for an authenticated A2A dispatch.",
            "idempotency_key": idempotency_key,
        })
        return work_items.reference_prefix(work_item_id), ""
    except work_items.WorkItemError as exc:
        return "", f"Shared work-item reference was omitted: {exc.safe_message}."


def _format_task_result(
    agent: str,
    reply: str,
    context_id: str,
    state: str,
    task_id: str,
    warning: str = "",
) -> str:
    short_state = state.replace("TASK_STATE_", "").replace("_", "-").lower()
    header = f"[{agent} · context {context_id}"
    if task_id:
        header += f" · task {task_id}"
    if state:
        header += f" · {short_state}"
    header += "]"
    if state in {protocol.STATE_SUBMITTED, protocol.STATE_WORKING}:
        body = (
            "ACCEPTED: the peer is still working on this task. Do not resend it. "
            f"Use a2a_task_status with agent '{agent}' and task_id '{task_id}' to retrieve the result."
        )
    else:
        body = reply or "(no text reply)"
    if state == protocol.STATE_INPUT_REQUIRED:
        body += f"\n\n(The peer needs more input — answer by calling a2a_call again with context_id '{context_id}'.)"
    if warning:
        body += f"\n\nWarning: {warning}"
    return f"{header}\n{body}"

def a2a_discover(args: dict, **_: Any) -> str:
    """Fetch and summarize a configured fleet peer's Agent Card."""
    if _a2a_chaining_blocked(str(args.get("agent") or "").strip()):
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    agent = str(args.get("agent") or "").strip()
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown configured peer '{agent}'."
    url = str(peer["url"])
    try:
        card = _fetch_card(url, _auth_header(peer.get("auth") or {}), int(peer.get("timeout", _DEFAULT_TIMEOUT)))
    except urllib.error.HTTPError as e:
        return f"Error: discovery of '{agent}' failed - HTTP {e.code}."
    except Exception:
        return f"Error: could not reach configured peer '{agent}'."
    caps = card.get("capabilities", {}) or {}
    skills = card.get("skills", []) or []
    auth = "yes" if card.get("security") else "no"
    proto = ", ".join(
        f"{i.get('protocolBinding', '?')} v{i.get('protocolVersion', '?')}"
        for i in (card.get("supportedInterfaces", []) or []) if isinstance(i, dict)
    ) or f"v{card.get('protocolVersion', '?')} (pre-1.0 card)"
    lines = [f"Agent: {card.get('name', '?')}", f"Description: {card.get('description', '')}", f"Configured peer: {agent}",
             f"Protocol: {proto}",
             f"Streaming: {bool(caps.get('streaming'))}  Push: {bool(caps.get('pushNotifications'))}  Auth required: {auth}",
             f"Skills ({len(skills)}):"]
    lines.extend(f"  - {s.get('name', s.get('id', '?'))}: {s.get('description', '')}" for s in skills[:20])
    return "\n".join(lines)


def a2a_call(args: dict, **_: Any) -> str:
    """Send a task to a reviewed configured peer; ``context_id`` continues a prior exchange."""
    if _a2a_chaining_blocked(str(args.get("agent") or args.get("agent_name") or "").strip()):
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    # Accept common aliases models reach for (observed live: 'agent_name').
    agent = str(args.get("agent") or args.get("agent_name") or args.get("name") or "").strip()
    message = str(args.get("message") or args.get("text") or args.get("task") or "").strip()
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    work_item_id = str(args.get("work_item_id") or "").strip()
    idempotency_key = str(args.get("idempotency_key") or "").strip()
    if not agent or not message:
        return "Error: both 'agent' and 'message' are required."
    hospice_related = _is_hospice_related(message)
    work_item_warning = ""
    if work_item_id:
        try:
            prefix, work_item_warning = _work_item_reference_or_create(work_item_id)
            message = prefix + message
        except work_items.WorkItemError as exc:
            return f"Error: {exc.safe_message}."
    if hospice_related and agent != "jeff":
        medical_on_jeff_case = False
        if agent == "medical_records" and work_item_id:
            try:
                medical_on_jeff_case = work_items.has_actor_status(
                    work_item_id, "jeff", "medical_records_requested"
                )
            except work_items.WorkItemError:
                medical_on_jeff_case = False
        if not medical_on_jeff_case:
            return (
                "Owner routing correction: hospice-related work routes to the configured peer 'jeff'. "
                "Medical Records may also participate through a shared work item after Jeff has recorded an entry."
            )
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'. Configure it under 'a2a_agents' in config.yaml. Direct URLs are not permitted."
    context_id = context_id or protocol.new_context_id()
    try:
        reply, reply_ctx, state, task_id = _send_task(
            agent,
            peer,
            message,
            context_id,
            request_id=idempotency_key,
        )
    except urllib.error.HTTPError as e:
        return _HTTP_CALL_ERRORS.get(e.code, "Error: call to '{agent}' failed — HTTP {code}.").format(agent=agent, code=e.code)
    except A2ASubmitTimeout as exc:
        return (
            f"[{agent} · context {exc.context_id} · delivery-unknown]\n"
            "The peer did not return a task receipt before the transport timeout. Do not resend: the peer may "
            "already be working. Use a2a_task_status with this context_id to recover the accepted task."
        )
    except ValueError as e:
        return str(e)
    except Exception as e:
        return f"Error: call to '{agent}' failed — {e}."
    return _format_task_result(agent, reply, reply_ctx, state, task_id, work_item_warning)


def a2a_task_status(args: dict, **_: Any) -> str:
    """Read a previously accepted task by id, or recover the newest task in a context."""
    if _a2a_chaining_blocked(str(args.get("agent") or "").strip()):
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    agent = str(args.get("agent") or "").strip()
    task_id = str(args.get("task_id") or args.get("taskId") or "").strip()
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not agent or not (task_id or context_id):
        return "Error: 'agent' and either 'task_id' or 'context_id' are required."
    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return f"Error: unknown agent '{agent}'."
    base_url = peer.get("url", "")
    headers = _auth_header(peer.get("auth", {}) or {})
    timeout = max(1, min(_POLL_HTTP_TIMEOUT, int(peer.get("timeout", _DEFAULT_TIMEOUT))))
    try:
        try:
            card = _fetch_card(base_url, headers, timeout)
        except Exception:
            card = None
        iface = _select_jsonrpc_interface(card)
        tenant = str(iface["tenant"]) if iface and iface.get("tenant") else str(peer.get("tenant") or "")
        tenant_params = {"tenant": tenant} if tenant else {}
        if task_id:
            payload = _task_rpc(base_url, card, headers, "GetTask", {"id": task_id, **tenant_params}, timeout)
        else:
            listing = _task_rpc(
                base_url,
                card,
                headers,
                "ListTasks",
                {"contextId": context_id, "pageSize": 1, "includeArtifacts": True, **tenant_params},
                timeout,
            )
            tasks = listing.get("tasks", []) if isinstance(listing, dict) else []
            if not tasks:
                return f"No task found for context '{context_id}'."
            payload = tasks[0]
        reply, reply_ctx, state, resolved_task_id = _task_fields(payload, context_id)
        return _format_task_result(agent, reply, reply_ctx, state, resolved_task_id or task_id)
    except urllib.error.HTTPError as exc:
        return _HTTP_CALL_ERRORS.get(
            exc.code,
            "Error: task status from '{agent}' failed — HTTP {code}.",
        ).format(agent=agent, code=exc.code)
    except Exception as exc:
        return f"Error: task status from '{agent}' failed — {exc}."


def a2a_list(args: dict | None = None, **_: Any) -> str:
    """List configured A2A peers, persisted conversations, and metrics."""
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    lines = []
    if peers:
        lines.append(f"Configured peers ({len(peers)}):")
        for name in _CANONICAL_PEER_LABELS:
            entry = peers.get(name)
            if not isinstance(entry, dict):
                continue
            caps = entry.get("capabilities", [])
            lines.append(f"  - {name} (auth: {(entry.get('auth') or {}).get('type', 'none')})"
                         + (f" caps: {', '.join(caps)}" if caps else ""))
            if description := str(entry.get("description") or "").strip():
                lines.append(f"    {description}")
    else:
        lines.append("No peers configured. Add them under 'a2a_agents' in config.yaml.")
    known = cfg.get("a2a_known_agents") or {}
    if isinstance(known, dict) and known:
        lines.append("")
        lines.append(f"Known but not connected ({len(known)}):")
        for name, entry in known.items():
            if not isinstance(entry, dict):
                continue
            lines.append(f"  - {name} ({str(entry.get('status') or 'not connected').strip()})")
            lines.append(f"    {str(entry.get('description') or 'known fleet agent').strip()}")
    if convos := protocol.list_conversations():
        lines.append("")
        lines.append(f"Persisted conversations ({len(convos)}) — recall with a2a_history:")
        lines.extend(f"  - {c}" for c in convos[:25])
    m = protocol.metrics.snapshot()
    lines.append("")
    lines.append(f"Metrics: {m['inbound_total']} in / {m['outbound_total']} out, {m['tasks_completed']} completed, "
                 f"{m['tasks_failed']} failed, {m['streams_started']} streams, {m['push_sent']} push sent, "
                 f"{m['anti_loop_triggers']} anti-loop, {m['rate_limit_triggers']} rate-limited, avg {m['avg_latency_ms']}ms")
    return "\n".join(lines)


def a2a_history(args: dict, **_: Any) -> str:
    """Recall a persisted A2A conversation (survives compaction/restarts)."""
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not context_id:
        return "Error: 'context_id' is required (see a2a_list for known conversations)."
    limit = max(1, min(_coerce_int(args.get("limit") or 50, 50), 200))
    messages = protocol.load_conversation(context_id, limit=limit)
    if not messages:
        return f"No persisted conversation for context '{context_id}'."
    lines = [f"Conversation {context_id} (last {len(messages)} messages):"]
    for m in messages:
        text = (m.get("text") or "").strip()
        lines.append(f"[{m.get('role', '?')}] {text[:1000] + ' …[truncated]' if len(text) > 1000 else text}")
    return "\n".join(lines)


def _match_peers_by_capability(capability: str) -> list[tuple[str, dict]]:
    """Configured peers that advertise the capability ('*' matches all)."""
    peers = _configured_peers()
    return [(name, peers[name]) for name in _CANONICAL_PEER_LABELS
            if isinstance(peers.get(name), dict)
            and (capability in (peers[name].get("capabilities", []) or []) or capability == "*")]


def _call_peer_sync(agent_name: str, peer_entry: dict, message: str, context_id: str = "") -> tuple[str, str]:
    """Call a single peer synchronously -> (agent_name, reply_text)."""
    try:
        reply, _ctx, _state, _task_id = _send_task(
            agent_name,
            _peer_from_entry(peer_entry),
            message,
            context_id,
        )
        return (agent_name, reply or "(no reply)")
    except Exception as e:
        return (agent_name, f"Error: {e}")


def a2a_orchestrate(args: dict, **_: Any) -> str:
    """Fan-out a task to peers matching a capability. Modes: ``all``, ``first`` (first successful),
    ``best`` (longest successful — coarse; use ``all`` to judge yourself)."""
    if _a2a_chaining_blocked():
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    capability = str(args.get("capability") or "").strip()
    message = str(args.get("message") or args.get("task") or "").strip()
    mode = str(args.get("mode") or "all").strip().lower()
    mode = mode if mode in ("all", "first", "best") else "all"
    if args.get("context_id"):
        return "Error: a2a_orchestrate starts an independent context for each peer; shared context_id is not supported."
    if not message:
        return "Error: 'message' is required."
    if not capability:
        return "Error: 'capability' is required (or use '*' for all peers)."
    if _is_hospice_related(message):
        jeff = _configured_peers().get("jeff")
        matches = [("jeff", jeff)] if isinstance(jeff, dict) else []
    else:
        matches = _match_peers_by_capability(capability)
    if not matches:
        return f"Error: no configured peers advertise capability '{capability}'."
    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(len(matches), _ORCHESTRATE_MAX_WORKERS)) as pool:
        futures = {}
        for name, entry in matches:
            worker_context = contextvars.copy_context()
            futures[pool.submit(worker_context.run, _call_peer_sync, name, entry, message)] = name
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
                if mode == "first" and not results[-1][1].startswith("Error:"):
                    for f in futures:  # good reply; cancel peers that haven't started
                        f.cancel()
                    break
            except Exception as e:
                results.append((name, f"Error: {e}"))
    results.sort(key=lambda r: r[0])  # deterministic output
    successes = [(name, reply) for name, reply in results if not reply.startswith("Error:")]
    if mode in ("best", "first"):
        if not successes:
            return "\n".join(["All peers failed:"] + [f"  {name}: {reply}" for name, reply in results])
        name, reply = max(successes, key=lambda r: len(r[1])) if mode == "best" else successes[0]
        return f"[{mode}: {name}]\n{reply}"
    return "\n".join([f"Orchestrated '{capability}' to {len(matches)} peer(s):"]
                     + [line for name, reply in results for line in (f"\n--- {name} ---", reply)])


def _str(description: str) -> dict:
    return {"type": "string", "description": description}


def _typed_metadata(fields: list[str], description: str, *, states: Optional[list[str]] = None) -> dict:
    value_schema = {"type": "string"}
    if states is not None:
        value_schema["enum"] = states
    return {
        "type": "object",
        "description": description,
        "properties": {field: dict(value_schema) for field in fields},
        "additionalProperties": False,
    }


# name -> (handler, description, properties, required)
_TOOLS: dict[str, tuple[Any, str, dict, list[str]]] = {
    "a2a_discover": (a2a_discover,
                     "Fetch and summarize a configured fleet peer's A2A Agent Card (its name, description, "
                     "capabilities, and skills).",
                     {"agent": {"type": "string", "enum": list(_CANONICAL_PEER_LABELS),
                                "description": "Configured fleet peer name."}}, ["agent"]),
    "a2a_call": (a2a_call,
                 "Send a natural-language task to a remote A2A agent and return its reply or a durable accepted "
                 "task receipt. Never resend an accepted task; use a2a_task_status with its task id. The agent is a peer "
                 "(any A2A-compliant framework), not a sub-agent you control. Pass 'context_id' from a previous "
                 "reply to continue a multi-turn exchange.",
                 {"agent": {"type": "string", "enum": list(_CANONICAL_PEER_LABELS),
                            "description": "Configured fleet peer name."},
                  "message": _str("The task / message to send the peer, in natural language."),
                  "context_id": _str("Optional: context id from a prior reply, to continue the conversation."),
                  "work_item_id": _str("Optional shared work-item reference; a missing valid reference is created automatically."),
                  "idempotency_key": _str("Optional stable key for retrying this exact logical dispatch without duplicate execution.")},
                 ["agent", "message"]),
    "a2a_task_status": (
        a2a_task_status,
        "Retrieve the current or terminal result for a previously accepted A2A task. Use task_id from a2a_call; "
        "use context_id only to recover a submission whose receipt timed out. Do not resend the original task.",
        {
            "agent": {"type": "string", "enum": list(_CANONICAL_PEER_LABELS),
                      "description": "Configured fleet peer name."},
            "task_id": _str("Preferred: durable task id returned by a2a_call."),
            "context_id": _str("Fallback: context id used for the original call."),
        },
        ["agent"],
    ),
    "a2a_list": (a2a_list, "List configured A2A peer agents, persisted A2A conversations, and metrics.", {}, []),
    "a2a_history": (a2a_history,
                    "Recall a persisted A2A conversation transcript by context_id (survives restarts and "
                    "context compaction). Use a2a_list to see known context ids.",
                    {"context_id": _str("Context id of the conversation to recall."),
                     "limit": {"type": "integer", "description": "Max messages to return (default 50, max 200)."}},
                    ["context_id"]),
    "a2a_orchestrate": (a2a_orchestrate,
                        "Fan-out a task to multiple peer agents by capability. Peers are matched from config.yaml "
                        "a2a_agents.*.capabilities. Modes: 'all' (return all replies), 'first' (first successful), "
                        "'best' (longest successful reply).",
                        {"capability": _str("Capability to match (e.g. 'research', 'code') or '*' for all peers."),
                         "message": _str("The task to send to all matching peers."),
                         "mode": {"type": "string", "enum": ["all", "first", "best"], "description": "How to aggregate results. Default: 'all'."}},
                        ["capability", "message"]),
}

_WORK_ITEM_TOOLS: dict[str, tuple[Any, str, dict, list[str]]] = {
    "work_item_read": (
        work_items.work_item_read_tool,
        "Read one shared fleet work item by its exact reference ID. The record contains append-only status entries written by Watson and connected peers.",
        {
            "work_item_id": _str("Exact shared work-item reference ID."),
            "after_sequence": {"type": "integer", "description": "Optional: return entries after this sequence number."},
            "limit": {"type": "integer", "description": "Optional: maximum entries to return (default and maximum 20)."},
        },
        ["work_item_id"],
    ),
    "work_item_append": (
        work_items.work_item_append_tool,
        "Append a factual status entry to a shared fleet work item. The first append creates the reference; later appends preserve prior entries.",
        {
            "work_item_id": _str("Shared work-item reference ID."),
            "status": _str("Short status label."),
            "note": _str("Optional factual note for the shared record."),
            "idempotency_key": _str("Required stable key; reuse only when retrying the exact same append."),
            "provenance": _typed_metadata(
                [
                    "originating_principal", "transport_authenticated_peer", "causal_reporter", "intake_context",
                    "intake_task", "producer_receipt", "local_session", "stage_invocation",
                    "dispatch_correlation_id", "queue_receipt", "return_channel", "operational_owner",
                    "owner_thread", "notification_target", "parent_item",
                ],
                "Optional typed provenance. Use originating_principal=unavailable when authorship is unproven; never infer it from the relay.",
            ),
            "gates": _typed_metadata(
                [
                    "account_unlock_state", "password_reset_state", "authentication_login_verification_state",
                    "credential_persistence_state", "credential_delivery_state", "human_captcha_gate_state",
                    "prepared_task_artifact_state", "side_effect_authorization_state",
                ],
                "Optional independent gate states; do not collapse them into the top-level status.",
                states=[
                    "unknown", "not_applicable", "not_requested", "pending", "prepared", "preserved", "blocked",
                    "failed", "lost_before_persistence", "complete", "verified", "delivered", "authorized",
                    "unauthorized",
                ],
            ),
        },
        ["work_item_id", "status", "idempotency_key"],
    ),
}


def _a2a_tools_available() -> bool:
    """check_fn: serve the client tools ONLY when the operator opted into A2A (peers under
    ``a2a_agents``, inbound platform enabled, or A2A_PORT set). Fail closed.

    Maintainer-directed (#95681): these registered unconditionally, so every session on every install paid
    ~561 tok/call for tools whose only possible output without config is 'no peers configured'. A2A is
    unrelated to Bot Mode (bots talk over gateway RPCs) — for most installs this toolset is foreign-agent
    plumbing they never enabled. Config adds mid-session surface at the next compaction (#97073).
    """
    cfg = {}
    with contextlib.suppress(Exception):
        cfg = _load_config()
        if cfg.get("a2a_agents"):
            return True
    try:
        if os.getenv("A2A_PORT"):
            return True
        a2a_cfg = (cfg.get("platforms") or {}).get("a2a") or {}
        return bool(isinstance(a2a_cfg, dict) and a2a_cfg.get("enabled"))
    except Exception:  # noqa: BLE001
        return False


def register_tools(ctx) -> None:
    """Register the client tools in the ``a2a`` toolset (config-gated)."""
    for name, (handler, description, properties, required) in _TOOLS.items():
        if name == "a2a_call":
            roster = _configured_peer_roster()
            if roster:
                description += "\n\nConfigured peers (factual roster):\n" + "\n".join(roster)
            expectations = _routing_expectations()
            if expectations:
                description += "\n\nOwner routing correction (only these exceptions are mandated):\n- " + "\n- ".join(expectations)
        parameters: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            parameters["required"] = required
        ctx.register_tool(name=name, toolset="a2a", handler=handler, description=description,
                          schema={"name": name, "description": description, "parameters": parameters},
                          emoji="\U0001f9e9", check_fn=_a2a_tools_available)  # puzzle piece
    for name, (handler, description, properties, required) in _WORK_ITEM_TOOLS.items():
        parameters: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            parameters["required"] = required
        ctx.register_tool(name=name, toolset="work_items", handler=handler, description=description,
                          schema={"name": name, "description": description, "parameters": parameters},
                          emoji="\U0001f4cb", check_fn=_a2a_tools_available)  # clipboard


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
from typing import TypedDict  # noqa: F401,E402
# ---- END PLUGIN-COMPAT ----
