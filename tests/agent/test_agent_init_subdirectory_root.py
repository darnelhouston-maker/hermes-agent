"""Regression coverage for the startup/subdirectory-hint cwd contract."""

from types import SimpleNamespace
from unittest.mock import patch

from agent.agent_init import _init_usage_state


def _agent(*, skip_context_files=False):
    return SimpleNamespace(skip_context_files=skip_context_files)


def test_cron_context_cwd_roots_tracker_and_prevents_duplicate_root_hint(tmp_path):
    terminal_root = tmp_path / "terminal-root"
    workspace = terminal_root / "workspace"
    workspace.mkdir(parents=True)
    (workspace / "AGENTS.md").write_text("synthetic-rule\n" * 4_000, encoding="utf-8")
    target = workspace / "first-tool.txt"
    target.write_text("synthetic", encoding="utf-8")
    agent = _agent()

    with patch("agent.runtime_cwd.resolve_context_cwd", return_value=workspace), \
            patch("agent.runtime_cwd.scope_terminal_cwd", return_value=str(terminal_root)) as terminal:
        _init_usage_state(agent)

    assert agent._subdirectory_hints.working_dir == workspace.resolve()
    terminal.assert_not_called()
    assert agent._subdirectory_hints.check_tool_call(
        "read_file", {"path": str(target)}
    ) is None


def test_terminal_cwd_is_the_fallback_when_context_cwd_is_unavailable(tmp_path):
    terminal_root = tmp_path / "terminal-root"
    terminal_root.mkdir()
    agent = _agent()

    with patch("agent.runtime_cwd.resolve_context_cwd", return_value=None), \
            patch("agent.runtime_cwd.scope_terminal_cwd", return_value=str(terminal_root)):
        _init_usage_state(agent)

    assert agent._subdirectory_hints.working_dir == terminal_root.resolve()
    assert agent._subdirectory_hints.enabled is True


def test_interactive_session_without_configured_cwd_falls_back_to_process_cwd(
        tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    agent = _agent()

    with patch("agent.runtime_cwd.resolve_context_cwd", return_value=None), \
            patch("agent.runtime_cwd.scope_terminal_cwd", return_value=""):
        _init_usage_state(agent)

    assert agent._subdirectory_hints.working_dir == tmp_path.resolve()


def test_skip_context_files_keeps_subdirectory_hint_injection_disabled(tmp_path):
    workspace = tmp_path / "workspace"
    subdirectory = workspace / "subdirectory"
    subdirectory.mkdir(parents=True)
    (subdirectory / "AGENTS.md").write_text("synthetic nested rule", encoding="utf-8")
    agent = _agent(skip_context_files=True)

    with patch("agent.runtime_cwd.resolve_context_cwd", return_value=workspace), \
            patch("agent.runtime_cwd.scope_terminal_cwd", return_value=""):
        _init_usage_state(agent)

    assert agent._subdirectory_hints.working_dir == workspace.resolve()
    assert agent._subdirectory_hints.enabled is False
    assert agent._subdirectory_hints.check_tool_call(
        "read_file", {"path": str(subdirectory / "file.py")}
    ) is None
