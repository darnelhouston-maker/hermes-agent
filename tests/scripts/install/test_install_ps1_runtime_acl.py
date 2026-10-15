"""Elevated Windows installs keep the per-user runtime usable after logon."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


pytestmark = pytest.mark.platforms("windows")
INSTALLER = Path(__file__).resolve().parents[3] / "scripts" / "install.ps1"


def _powershell() -> str:
    powershell = shutil.which("powershell")
    assert powershell, "Windows PowerShell 5.1 is part of every supported Windows"
    return powershell


def test_runtime_acl_grants_the_current_sid_once(tmp_path: Path) -> None:
    home = tmp_path / "home"
    install = tmp_path / "checkout"
    log = tmp_path / "icacls.log"
    script = rf"""
$ErrorActionPreference = "Stop"
$env:ACL_TEST_LOG = '{log}'
function icacls {{
    Add-Content -LiteralPath $env:ACL_TEST_LOG -Value ($args -join "|")
    $global:LASTEXITCODE = 0
}}
. '{INSTALLER}' -HermesHome '{home}' -InstallDir '{install}'
Ensure-InteractiveRuntimeAcl
Ensure-InteractiveRuntimeAcl
"""
    result = subprocess.run(
        [_powershell(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    [call] = log.read_text(encoding="utf-8-sig").splitlines()
    fields = call.split("|")
    assert str(home / "tools") in fields[0]
    assert fields[1] == "/grant:r"
    assert fields[2].startswith("*S-1-")
    assert fields[2].endswith(":(OI)(CI)(M)")
    assert fields[-3:] == ["/T", "/C", "/Q"]


def test_bootstrap_repairs_acl_before_resolving_uv() -> None:
    source = INSTALLER.read_text(encoding="utf-8-sig")
    body = source.split("function Get-BootstrapPython", 1)[1].split(
        "function Invoke-BootstrapPm", 1
    )[0]
    assert body.index("Ensure-InteractiveRuntimeAcl") < body.index("$uv = Get-Uv")
