"""
A2A client tools — let the Hermes agent talk to *other* agents as a peer.

Tools (registered in the ``a2a`` toolset):
  - a2a_discover(url)         -> fetch + summarize a peer's Agent Card
  - a2a_call(agent, message)  -> send a task to a peer, return its reply
  - a2a_list()                -> list configured peers + persisted conversations
  - a2a_history(context_id)   -> recall a persisted A2A conversation
  - a2a_orchestrate(...)      -> fan-out task to multiple peers by capability

Peers are resolved from config.yaml under ``a2a_agents``::

    a2a_agents:
      researcher:
        url: "http://localhost:9999"
        auth: { type: bearer, token: "sk-..." }
        timeout: 120
        capabilities: [web_search, research]

Transport is stdlib urllib (no a2a-sdk dependency). The wire format is the A2A
v1.0 JSON-RPC ``message/send`` method; replies from v0.3 peers still parse.
"""

from __future__ import annotations

import hashlib
import json
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, TypedDict

from . import protocol, security, work_items

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 120
_POLL_INTERVAL = 2.0
_POLL_HTTP_TIMEOUT = 30
_ORCHESTRATE_MAX_WORKERS = 6  # max parallel peers for fan-out
_PEER_PORTS = {"watson": 19990, "nova": 19903}
_ALLOWED_PEERS = set(_PEER_PORTS)
_CALL_TERMINAL_STATES = protocol.TERMINAL_STATES | {protocol.STATE_INPUT_REQUIRED}


class A2ASubmitTimeout(TimeoutError):
    """The initial request timed out before a durable task receipt arrived."""

    def __init__(self, agent: str, context_id: str):
        super().__init__(f"A2A submission to {agent} timed out before a task receipt")
        self.agent = agent
        self.context_id = context_id


def _a2a_chaining_blocked(target: str = "") -> bool:
    '''Block outbound network A2A from an A2A-originated agent turn.'''
    try:
        from gateway.session_context import get_session_env

        return (get_session_env("HERMES_SESSION_PLATFORM", "") or "").strip().lower() == "a2a"
    except Exception:
        return True


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


# --------------------------------------------------------------------------
# Peer resolution
# --------------------------------------------------------------------------

def _load_config() -> dict:
    try:
        from hermes_cli.config import load_config
        return load_config() or {}
    except Exception:
        return {}


def _resolve_peer(agent: str) -> Optional[dict]:
    """Resolve only the configured Watson hub over its loopback/private bridge."""
    if agent not in _ALLOWED_PEERS:
        return None
    entry = ((_load_config().get("a2a_agents") or {}).get(agent))
    if not isinstance(entry, dict):
        return None
    url = str(entry.get("url") or "").strip()
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "172.18.0.1"}:
        return None
    if port != _PEER_PORTS.get(agent) or parsed.username is not None or parsed.password is not None:
        return None
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return None
    auth = entry.get("auth", {}) or {}
    if auth.get("type") != "bearer" or not str(auth.get("token") or "").strip():
        return None
    return {
        "url": url,
        "auth": auth,
        "timeout": int(entry.get("timeout", _DEFAULT_TIMEOUT)),
        "capabilities": entry.get("capabilities", []) or [],
        "tenant": entry.get("tenant", ""),
    }


def _auth_header(auth: dict) -> dict:
    if auth and auth.get("type") == "bearer" and auth.get("token"):
        return {"Authorization": f"Bearer {auth['token']}"}
    return {}


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def _http_get_json(url: str, headers: dict, timeout: int) -> dict:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with _NO_REDIRECT_OPENER.open(req, timeout=timeout) as resp:  # noqa: S310 (configured peers)
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, body: dict, headers: dict, timeout: int) -> dict:
    data = json.dumps(body).encode("utf-8")
    hdrs = {"Content-Type": "application/json", "A2A-Version": protocol.PROTOCOL_VERSION, **headers}
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with _NO_REDIRECT_OPENER.open(req, timeout=timeout) as resp:  # noqa: S310 (configured peers)
        return json.loads(resp.read().decode("utf-8"))


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
    if payload.get("id") and not isinstance(payload.get("contextId"), str):
        raise ValueError("Peer task receipt has no explicit context identity; do not resend.")
    if payload.get("id") and not payload.get("contextId", "").strip():
        raise ValueError("Peer task receipt has no explicit context identity; do not resend.")
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


def _card_url(base_url: str) -> str:
    # A2A v1.0 canonical discovery path. v0.2 used agent.json; servers may
    # still serve that as a legacy alias, but clients should prefer this.
    return base_url.rstrip("/") + "/.well-known/agent-card.json"


def _legacy_card_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/.well-known/agent.json"


def _fetch_card(base_url: str, headers: dict, timeout: int) -> dict:
    try:
        return _http_get_json(_card_url(base_url), headers, timeout)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    return _http_get_json(_legacy_card_url(base_url), headers, timeout)


def _select_jsonrpc_interface(card: Optional[dict]) -> Optional[dict]:
    if isinstance(card, dict):
        for iface in card.get("supportedInterfaces", []) or []:
            if isinstance(iface, dict) and iface.get("protocolBinding") == "JSONRPC" and iface.get("url"):
                return iface
    return None


def _rpc_url(base_url: str, card: Optional[dict]) -> str:
    """Accept an advertised RPC URL only when it has the configured origin."""
    base = urllib.parse.urlsplit(base_url)
    candidates = []
    iface = _select_jsonrpc_interface(card)
    if iface:
        candidates.append(str(iface.get("url") or ""))
    if isinstance(card, dict):
        candidates.append(str(card.get("url") or ""))
    for candidate in candidates:
        try:
            resolved = urllib.parse.urljoin(base_url.rstrip("/") + "/", candidate)
            parsed = urllib.parse.urlsplit(resolved)
            if (parsed.scheme, parsed.hostname, parsed.port or 80) == (
                base.scheme, base.hostname, base.port or 80
            ) and not parsed.username and not parsed.password and not parsed.fragment:
                return resolved
        except ValueError:
            continue
    return base_url.rstrip("/")


def _interface_tenant(card: Optional[dict], peer: dict) -> str:
    iface = _select_jsonrpc_interface(card)
    if iface and iface.get("tenant"):
        return str(iface["tenant"])
    return str(peer.get("tenant") or "")


# --------------------------------------------------------------------------
# Shared send path (used by a2a_call and a2a_orchestrate)
# --------------------------------------------------------------------------

def _short_state(state: str) -> str:
    """TASK_STATE_COMPLETED -> completed (also passes through v0.3 states)."""
    return state.replace("TASK_STATE_", "").replace("_", "-").lower() if state else ""


def _send_task(
    agent_label: str,
    peer: dict,
    message: str,
    context_id: str,
    request_id: str = "",
) -> tuple[str, str, str, str]:
    """Send one detachable task and return its reply or durable receipt."""
    if _a2a_chaining_blocked(agent_label):
        raise RuntimeError("A2A chaining is disabled for A2A-origin sessions.")
    return _send_task_uncontrolled(
        agent_label,
        peer,
        message,
        context_id,
        request_id=request_id,
    )


def _send_task_uncontrolled(
    agent_label: str,
    peer: dict,
    message: str,
    context_id: str,
    request_id: str = "",
) -> tuple[str, str, str, str]:
    """Submit a detachable task, then poll it within the caller wait budget."""
    base_url = peer.get("url", "")
    headers = _auth_header(peer.get("auth", {}) or {})
    timeout = int(peer.get("timeout", _DEFAULT_TIMEOUT))

    # Best-effort card fetch (to learn the rpc URL); non-fatal on failure.
    card = None
    try:
        card = _fetch_card(base_url, headers, min(timeout, 30))
    except Exception:
        pass

    ctx = context_id or protocol.new_context_id()
    wire_message = message
    message_id = _stable_message_id(request_id)
    # v1.0: contextId lives inside the Message, not at the params top level.
    rpc_body = {
        "jsonrpc": "2.0",
        "id": protocol.new_task_id(),
        "method": "SendMessage",
        "params": {
            "message": protocol.text_message(
                protocol.ROLE_USER,
                wire_message,
                context_id=ctx,
                message_id=message_id,
            ),
            "configuration": {"blocking": False},
        },
    }

    tenant = _interface_tenant(card, peer)
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
        err = resp["error"]
        raise ValueError(f"Peer '{agent_label}' returned an error: {err.get('message', err)}")

    reply, reply_ctx, state, task_id = _task_fields(resp.get("result", {}), ctx)
    if reply_ctx != ctx:
        raise ValueError(f"Peer '{agent_label}' returned a mismatched conversation context.")
    if task_id:
        from .task_archive import TaskArchive
        endpoint = hashlib.sha256(json.dumps([base_url, tenant]).encode()).hexdigest()
        try:
            TaskArchive().bind_alias(agent_label, endpoint, rpc_body["id"], task_id, reply_ctx)
        except Exception as exc:
            logger.warning("A2A receipt alias persistence failed (%s); retain returned canonical receipt, do not resend", type(exc).__name__)
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
    # Artifacts first (final output), then status message (interim/clarify).
    for artifact in result.get("artifacts", []) or []:
        txt = protocol.extract_text(artifact)
        if txt:
            return txt
    status = result.get("status", {}) or {}
    msg = status.get("message")
    if msg:
        return protocol.extract_text(msg)
    # Bare message result (message/send may return a Message instead of a Task)
    return protocol.extract_text(result)


# --------------------------------------------------------------------------
# Tool handlers
# --------------------------------------------------------------------------

def a2a_discover(args: dict, **_: Any) -> str:
    """Fetch and summarize Watson's configured Agent Card."""
    if _a2a_chaining_blocked(str(args.get("agent") or "").strip()):
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    agent = str(args.get("agent") or "").strip()
    peer = _resolve_peer(agent)
    if not peer:
        return f"Error: unknown configured peer '{agent}'."
    url = str(peer["url"])
    try:
        card = _fetch_card(url, _auth_header(peer.get("auth") or {}), int(peer.get("timeout", _DEFAULT_TIMEOUT)))
    except urllib.error.HTTPError as e:
        return f"Error: discovery failed — HTTP {e.code}."
    except Exception as e:
        return f"Error: could not reach configured peer '{agent}' — {e}."

    name = card.get("name", "?")
    desc = card.get("description", "")
    caps = card.get("capabilities", {}) or {}
    skills = card.get("skills", []) or []
    auth = "yes" if card.get("security") else "no"
    ifaces = card.get("supportedInterfaces", []) or []
    proto = ", ".join(
        f"{i.get('protocolBinding', '?')} v{i.get('protocolVersion', '?')}"
        for i in ifaces if isinstance(i, dict)
    ) or f"v{card.get('protocolVersion', '?')} (pre-1.0 card)"
    lines = [
        f"Agent: {name}", f"Description: {desc}", f"Configured peer: {agent}",
        f"Protocol: {proto}",
        f"Streaming: {bool(caps.get('streaming'))}  Push: {bool(caps.get('pushNotifications'))}  Auth required: {auth}",
        f"Skills ({len(skills)}):",
    ]
    for skill in skills[:20]:
        lines.append(f"  - {skill.get('name', skill.get('id', '?'))}: {skill.get('description', '')}")
    return "\n".join(lines)


def a2a_call(args: dict, **_: Any) -> str:
    """Send a task to a peer agent and return its reply.

    ``agent`` is a configured peer name (from ``a2a_agents``) .
    ``context_id`` continues a prior exchange (multi-turn) when provided.
    """
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
    if work_item_id:
        try:
            prefix, warning = _work_item_reference_or_create(work_item_id)
            message = prefix + message
        except work_items.WorkItemError as exc:
            return f"Error: {exc.safe_message}."
    else:
        warning = ""

    peer = _resolve_peer(agent)
    if not peer or not peer.get("url"):
        return (
            f"Error: unknown agent '{agent}'. Configure it under 'a2a_agents' in "
            f"config.yaml. Direct URLs are not permitted."
        )

    try:
        reply, reply_ctx, state, task_id = _send_task(
            agent,
            peer,
            message,
            context_id,
            request_id=idempotency_key,
        )
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return f"Error: peer '{agent}' rejected auth (HTTP {e.code}). Check the configured token."
        if e.code == 429:
            return f"Error: peer '{agent}' rate limited us (HTTP 429). Retry later."
        return f"Error: call to '{agent}' failed — HTTP {e.code}."
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

    return _format_task_result(agent, reply, reply_ctx, state, task_id, warning)


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
    header = f"[{agent} · context {context_id}"
    if task_id:
        header += f" · task {task_id}"
    if state:
        header += f" · {_short_state(state)}"
    header += "]"
    if state in {protocol.STATE_SUBMITTED, protocol.STATE_WORKING}:
        body = (
            "ACCEPTED: the peer is still working on this task. Do not resend it. "
            f"Use a2a_task_status with agent '{agent}' and task_id '{task_id}' to retrieve the result."
        )
    else:
        body = reply or "(no text reply)"
    if state == protocol.STATE_INPUT_REQUIRED:
        body += f"\n\n(The peer needs more input — answer with context_id '{context_id}'.)"
    if warning:
        body += f"\n\nWarning: {warning}"
    return f"{header}\n{body}"


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
        tenant = _interface_tenant(card, peer)
        tenant_params = {"tenant": tenant} if tenant else {}
        if task_id:
            from .task_archive import lookup_alias
            endpoint = hashlib.sha256(json.dumps([base_url, tenant]).encode()).hexdigest()
            binding = lookup_alias(agent, endpoint, task_id)
            if binding:
                task_id, bound_context = binding
                if context_id and context_id != bound_context:
                    return "Error: task receipt context mismatch; do not resend."
                context_id = bound_context
            payload = _task_rpc(base_url, card, headers, "GetTask", {"id": task_id, **tenant_params}, timeout)
        else:
            listing = _task_rpc(
                base_url, card, headers, "ListTasks",
                {"contextId": context_id, "pageSize": 1, "includeArtifacts": True, **tenant_params},
                timeout,
            )
            tasks = listing.get("tasks", []) if isinstance(listing, dict) else []
            if not tasks:
                return f"No task found for context '{context_id}'."
            payload = tasks[0]
        reply, reply_ctx, state, resolved_task_id = _task_fields(payload, context_id)
        if (task_id and resolved_task_id != task_id) or (context_id and reply_ctx != context_id):
            return "Error: peer task receipt identity mismatch; do not resend."
        return _format_task_result(agent, reply, reply_ctx, state, resolved_task_id or task_id)
    except urllib.error.HTTPError as exc:
        if exc.code == 429 and not args.get("_retried_429"):
            time.sleep(1.0)
            return a2a_task_status({**args, "_retried_429": True})
        return f"Error: task status from '{agent}' failed — HTTP {exc.code}."
    except Exception as exc:
        return f"Error: task status from '{agent}' failed — {exc}."


def a2a_list(args: dict | None = None, **_: Any) -> str:
    """List configured A2A peers and any persisted conversations."""
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    lines = []
    if peers:
        lines.append(f"Configured peers ({len(peers)}):")
        for name, entry in peers.items():
            auth = (entry.get("auth") or {}).get("type", "none")
            caps = entry.get("capabilities", [])
            cap_str = f" caps: {', '.join(caps)}" if caps else ""
            lines.append(f"  - {name}: {entry.get('url', '?')} (auth: {auth}){cap_str}")
    else:
        lines.append("No peers configured. Add them under 'a2a_agents' in config.yaml.")

    convos = protocol.list_conversations()
    if convos:
        lines.append("")
        lines.append(f"Persisted conversations ({len(convos)}) — recall with a2a_history:")
        for c in convos[:25]:
            lines.append(f"  - {c}")

    # Show metrics snapshot
    m = protocol.metrics.snapshot()
    lines.append("")
    lines.append(f"Metrics: {m['inbound_total']} in / {m['outbound_total']} out, "
                 f"{m['tasks_completed']} completed, {m['tasks_failed']} failed, "
                 f"{m['streams_started']} streams, {m['push_sent']} push sent, "
                 f"{m['anti_loop_triggers']} anti-loop, {m['rate_limit_triggers']} rate-limited, "
                 f"avg {m['avg_latency_ms']}ms")

    return "\n".join(lines)


def a2a_history(args: dict, **_: Any) -> str:
    """Recall a persisted A2A conversation by context_id.

    This is how prior A2A exchanges survive compaction/restarts: every turn is
    written to ~/.hermes/a2a_conversations/<context>.jsonl and can be reloaded
    here.
    """
    context_id = str(args.get("context_id") or args.get("contextId") or "").strip()
    if not context_id:
        return "Error: 'context_id' is required (see a2a_list for known conversations)."
    try:
        limit = max(1, min(int(args.get("limit") or 50), 200))
    except (ValueError, TypeError):
        limit = 50
    messages = protocol.load_conversation(context_id, limit=limit)
    if not messages:
        return f"No persisted conversation for context '{context_id}'."
    lines = [f"Conversation {context_id} (last {len(messages)} messages):"]
    for m in messages:
        role = m.get("role", "?")
        text = (m.get("text") or "").strip()
        if len(text) > 1000:
            text = text[:1000] + " …[truncated]"
        lines.append(f"[{role}] {text}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# a2a_orchestrate: capability-based routing with fan-out
# --------------------------------------------------------------------------

def _match_peers_by_capability(capability: str) -> list[tuple[str, dict]]:
    """Find configured peers that advertise the given capability."""
    cfg = _load_config()
    peers = cfg.get("a2a_agents") or {}
    matches = []
    for name in _ALLOWED_PEERS:
        entry = peers.get(name)
        if not isinstance(entry, dict):
            continue
        caps = entry.get("capabilities", []) or []
        if capability in caps or capability == "*":
            matches.append((name, entry))
    return matches


def _call_peer_sync(agent_name: str, peer_entry: dict, message: str, context_id: str = "") -> tuple[str, str]:
    """Call a single peer synchronously. Returns (agent_name, reply_text)."""
    try:
        peer = _resolve_peer(agent_name)
        if not peer:
            return (agent_name, "Error: peer is not the configured Watson hub.")
        reply, _ctx, _state, _task_id = _send_task(agent_name, peer, message, context_id)
        return (agent_name, reply or "(no reply)")
    except Exception as e:
        return (agent_name, f"Error: {e}")


def a2a_orchestrate(args: dict, **_: Any) -> str:
    """Fan-out a task to multiple peer agents by capability.

    Modes:
      - ``all``: send to all peers matching the capability, return all replies.
      - ``first``: send to all matching peers, return the first successful reply.
      - ``best``: send to all, return the longest successful reply (a coarse
        detail heuristic — use ``all`` when you want to judge yourself).

    Configured peers advertise capabilities in config.yaml::

      a2a_agents:
        researcher:
          url: "http://localhost:9991"
          capabilities: [web_search, research]
        coder:
          url: "http://localhost:9992"
          capabilities: [code, debug]
    """
    if _a2a_chaining_blocked():
        return "Error: A2A chaining is disabled for A2A-origin sessions."
    capability = str(args.get("capability") or "").strip()
    message = str(args.get("message") or args.get("task") or "").strip()
    mode = str(args.get("mode") or "all").strip().lower()
    context_id = str(args.get("context_id") or "").strip()

    if not message:
        return "Error: 'message' is required."
    if not capability:
        return "Error: 'capability' is required (or use '*' for all peers)."

    matches = _match_peers_by_capability(capability)
    if not matches:
        return f"Error: no configured peers advertise capability '{capability}'."

    if mode not in ("all", "first", "best"):
        mode = "all"

    # Fan-out
    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(len(matches), _ORCHESTRATE_MAX_WORKERS)) as pool:
        futures = {
            pool.submit(_call_peer_sync, name, entry, message, context_id): name
            for name, entry in matches
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
                if mode == "first" and not results[-1][1].startswith("Error:"):
                    # Got a good reply; cancel peers that haven't started yet.
                    for f in futures:
                        f.cancel()
                    break
            except Exception as e:
                results.append((name, f"Error: {e}"))

    # Sort results by peer name for deterministic output
    results.sort(key=lambda r: r[0])
    successes = [(name, reply) for name, reply in results if not reply.startswith("Error:")]

    def _all_failed() -> str:
        lines = ["All peers failed:"]
        for name, reply in results:
            lines.append(f"  {name}: {reply}")
        return "\n".join(lines)

    if mode == "best":
        if not successes:
            return _all_failed()
        best = max(successes, key=lambda r: len(r[1]))
        return f"[best: {best[0]}]\n{best[1]}"
    elif mode == "first":
        if not successes:
            return _all_failed()
        name, reply = successes[0]
        return f"[first: {name}]\n{reply}"
    else:  # mode == "all"
        lines = [f"Orchestrated '{capability}' to {len(matches)} peer(s):"]
        for name, reply in results:
            lines.append(f"\n--- {name} ---")
            lines.append(reply)
        return "\n".join(lines)


# --------------------------------------------------------------------------
# Tool schemas + registration
# --------------------------------------------------------------------------

_FunctionSchema = TypedDict("_FunctionSchema", {"name": str, "description": str, "parameters": dict[str, Any]}, total=False)
_ToolSchema = TypedDict("_ToolSchema", {"type": str, "function": _FunctionSchema}, total=False)
_SCHEMAS: dict[str, _ToolSchema] = {
    "a2a_discover": {
        "type": "function",
        "function": {
            "name": "a2a_discover",
            "description": "Fetch and summarize the configured Watson hub's Agent Card.",
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "enum": sorted(_ALLOWED_PEERS), "description": "Configured fleet peer."},
                },
                "required": ["agent"],
            },
        },
    },
    "a2a_call": {
        "type": "function",
        "function": {
            "name": "a2a_call",
            "description": (
                "Send a natural-language task to a remote A2A agent and return "
                "its reply or a durable accepted task receipt. Never resend an accepted task; "
                "use a2a_task_status with its task id. The agent is a peer (any A2A-compliant framework), "
                "not a sub-agent you control. Pass 'context_id' from a previous "
                "reply to continue a multi-turn exchange. Only the configured Watson hub is available."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "enum": sorted(_ALLOWED_PEERS), "description": "Configured fleet peer."},
                    "message": {"type": "string", "description": "The task / message to send the peer, in natural language."},
                    "context_id": {"type": "string", "description": "Optional: context id from a prior reply, to continue the conversation."},
                    "work_item_id": {
                        "type": "string",
                        "description": "Optional shared work-item reference; a missing valid reference is created automatically.",
                    },
                    "idempotency_key": {"type": "string", "description": "Optional stable key for retrying this logical dispatch without duplicate execution."},
                },
                "required": ["agent", "message"],
            },
        },
    },
    "a2a_task_status": {
        "type": "function",
        "function": {
            "name": "a2a_task_status",
            "description": "Retrieve a previously accepted A2A task. Do not resend the original task.",
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "enum": sorted(_ALLOWED_PEERS), "description": "Configured fleet peer."},
                    "task_id": {"type": "string", "description": "Preferred durable task id returned by a2a_call."},
                    "context_id": {"type": "string", "description": "Fallback context id for receipt recovery."},
                },
                "required": ["agent"],
            },
        },
    },
    "a2a_list": {
        "type": "function",
        "function": {
            "name": "a2a_list",
            "description": "List configured A2A peer agents, persisted A2A conversations, and metrics.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    "a2a_history": {
        "type": "function",
        "function": {
            "name": "a2a_history",
            "description": (
                "Recall a persisted A2A conversation transcript by context_id "
                "(survives restarts and context compaction). Use a2a_list to "
                "see known context ids."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "context_id": {"type": "string", "description": "Context id of the conversation to recall."},
                    "limit": {"type": "integer", "description": "Max messages to return (default 50, max 200)."},
                },
                "required": ["context_id"],
            },
        },
    },
    "a2a_orchestrate": {
        "type": "function",
        "function": {
            "name": "a2a_orchestrate",
            "description": (
                "Fan-out a task to multiple peer agents by capability. Peers are "
                "matched from config.yaml a2a_agents.*.capabilities. Modes: 'all' "
                "(return all replies), 'first' (first successful), 'best' (longest "
                "successful reply)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "capability": {"type": "string", "description": "Capability to match (e.g. 'research', 'code') or '*' for all peers."},
                    "message": {"type": "string", "description": "The task to send to all matching peers."},
                    "mode": {"type": "string", "enum": ["all", "first", "best"], "description": "How to aggregate results. Default: 'all'."},
                    "context_id": {"type": "string", "description": "Optional: shared context id for all peers."},
                },
                "required": ["capability", "message"],
            },
        },
    },
}

_WORK_ITEM_SCHEMAS: dict[str, _ToolSchema] = {
    "work_item_read": {
        "type": "function",
        "function": {
            "name": "work_item_read",
            "description": (
                "Read one shared fleet work item by its exact reference ID. The record contains "
                "append-only status entries written by Watson and connected peers."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {"type": "string", "description": "Exact shared work-item reference ID."},
                    "after_sequence": {"type": "integer", "description": "Optional: return entries after this sequence number."},
                    "limit": {"type": "integer", "description": "Optional: maximum entries to return (default and maximum 20; paginate with after_sequence)."},
                },
                "required": ["work_item_id"],
            },
        },
    },
    "work_item_append": {
        "type": "function",
        "function": {
            "name": "work_item_append",
            "description": (
                "Append a factual status entry to a shared fleet work item. The first append creates "
                "the reference; later appends preserve every peer's prior entries."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {"type": "string", "description": "Shared work-item reference ID."},
                    "status": {"type": "string", "description": "Short status label."},
                    "note": {"type": "string", "description": "Optional factual note for the shared record."},
                    "idempotency_key": {
                        "type": "string",
                        "description": "Required stable key; reuse the same key only when retrying the exact same append.",
                    },
                },
                "required": ["work_item_id", "status", "idempotency_key"],
            },
        },
    },
}

_HANDLERS = {
    "a2a_discover": a2a_discover,
    "a2a_call": a2a_call,
    "a2a_task_status": a2a_task_status,
    "a2a_list": a2a_list,
    "a2a_history": a2a_history,
    "a2a_orchestrate": a2a_orchestrate,
}

_WORK_ITEM_HANDLERS = {
    "work_item_read": work_items.work_item_read_tool,
    "work_item_append": work_items.work_item_append_tool,
}


def _a2a_tools_available() -> bool:
    """Expose the client tools only when the operator configured A2A."""
    cfg = {}
    try:
        cfg = _load_config()
        if cfg.get("a2a_agents"):
            return True
    except Exception:
        pass
    try:
        import os as _os

        if _os.getenv("A2A_PORT"):
            return True
        a2a_cfg = ((cfg.get("platforms") or {}).get("a2a") or {})
        return isinstance(a2a_cfg, dict) and bool(a2a_cfg.get("enabled"))
    except Exception:
        return False


def register_tools(ctx) -> None:
    """Register the client tools in the ``a2a`` toolset."""
    for name, schema in _SCHEMAS.items():
        function_schema = schema["function"]
        ctx.register_tool(
            name=name,
            toolset="a2a",
            schema=function_schema,
            handler=_HANDLERS[name],
            description=function_schema["description"],
            emoji="\U0001f9e9",  # puzzle piece
            check_fn=_a2a_tools_available,
        )
    for name, schema in _WORK_ITEM_SCHEMAS.items():
        function_schema = schema["function"]
        ctx.register_tool(
            name=name,
            toolset="work_items",
            schema=function_schema,
            handler=_WORK_ITEM_HANDLERS[name],
            description=function_schema["description"],
            emoji="\U0001f4cb",  # clipboard
            check_fn=_a2a_tools_available,
        )
