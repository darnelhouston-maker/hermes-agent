"""Tests for shared tool result classification helpers."""

import json

from agent.tool_result_classification import (
    file_mutation_result_landed,
    parse_json_tool_result,
)


def test_write_file_with_nested_lint_error_counts_as_landed():
    result = json.dumps({
        "bytes_written": 12,
        "lint": {"status": "error", "output": "SyntaxError: invalid syntax"},
    })

    assert file_mutation_result_landed("write_file", result) is True


def test_json_tool_result_accepts_only_allowlisted_host_annotation():
    result = (
        '{"success": true, "name": "synthetic", "content": "loaded"}'
        "\n\n[Tool loop warning: idempotent_no_progress_warning; count=2; "
        "skill_view returned the same result 2 times.]"
    )

    assert parse_json_tool_result(result) is None
    assert parse_json_tool_result(result, allow_host_annotations=True) == {
        "success": True,
        "name": "synthetic",
        "content": "loaded",
    }


def test_json_tool_result_rejects_unknown_suffix_and_multiple_json_values():
    assert parse_json_tool_result(
        '{"success": true}\n\nuntrusted suffix', allow_host_annotations=True
    ) is None
    assert parse_json_tool_result(
        '{"success": true}{"success": true}', allow_host_annotations=True
    ) is None






def test_side_effect_classification_keeps_session_mutations():
    from agent.tool_result_classification import tool_may_have_side_effect

    assert tool_may_have_side_effect("todo") is True
    assert tool_may_have_side_effect("memory") is True
    assert tool_may_have_side_effect("write_file") is True
    assert tool_may_have_side_effect("mcp_unknown") is True
    assert tool_may_have_side_effect("read_file") is False
    assert tool_may_have_side_effect("web_search") is False
