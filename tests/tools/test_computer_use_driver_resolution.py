"""Explicit driver paths resolve through the native executable contract."""
import os


def test_cua_driver_cmd_env_override_is_resolved_dynamically(tmp_path, monkeypatch):
    from tools.computer_use import cua_backend, cua_backend_driver

    driver = tmp_path / ("custom-cua-driver.exe" if os.name == "nt" else "custom-cua-driver")
    driver.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    driver.chmod(0o755)
    monkeypatch.setenv("HERMES_CUA_DRIVER_CMD", str(driver))
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    assert cua_backend_driver.resolve_cua_driver_cmd() == str(driver)
    assert cua_backend.cua_driver_binary_available() is True
