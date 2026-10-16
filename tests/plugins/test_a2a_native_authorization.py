"""Authenticated A2A peers must survive the durable native admission boundary."""
import asyncio
from copy import copy
import os
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from gateway.session_envelope import prepare_native
from gateway.session_ingress_context import native_callback, register_transport_home
from gateway.session_ingress_context import reauthorize_roles, restore_provenance
from gateway.session_authority import initialize_session_authority
from gateway.run import GatewayRunner
from hermes_state_runtime import RuntimeStoreError
from plugins.platforms.a2a.adapter import A2AAdapter


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def connector(monkeypatch):
    monkeypatch.setenv('A2A_HOST', '0.0.0.0')
    monkeypatch.setenv('A2A_PEER_TOKENS', 'fixture-peer:fixture-token')
    monkeypatch.setenv('A2A_TRUSTED_PEERS', 'fixture-peer')
    monkeypatch.delenv('A2A_BEARER_TOKEN', raising=False)
    monkeypatch.delenv('A2A_ALLOW_ALL_USERS', raising=False)
    return A2AAdapter(PlatformConfig(enabled=True))


def source(connector, peer='fixture-peer'):
    value = connector.build_source(chat_id='synthetic-context', chat_type='dm',
                                   user_id=peer, user_name=peer)
    value.role_authorized = True
    return value


@pytest.mark.anyio
async def test_current_authenticated_peer_revalidates(connector):
    assert await connector.reauthorize_native_roles(source(connector)) is True


@pytest.mark.anyio
@pytest.mark.parametrize('change', ['untrusted', 'revoked_token', 'foreign_peer', 'wrong_name', 'group'])
async def test_changed_or_unbound_identity_fails_closed(connector, monkeypatch, change):
    value = source(connector)
    changes = {
        'untrusted': lambda: monkeypatch.setenv('A2A_TRUSTED_PEERS', 'other-peer'),
        'revoked_token': lambda: monkeypatch.setenv('A2A_PEER_TOKENS', 'fixture-peer:new-fixture-token'),
        'foreign_peer': lambda: (setattr(value, 'user_id', 'other-peer'), setattr(value, 'user_name', 'other-peer')),
        'wrong_name': lambda: setattr(value, 'user_name', 'other-peer'),
        'group': lambda: setattr(value, 'chat_type', 'group'),
    }
    changes[change]()
    assert await connector.reauthorize_native_roles(value) is False


def test_connector_generation_changes_without_leaking_token(connector, monkeypatch):
    old = GatewayRunner._adapter_credential_fingerprint(connector)
    assert old and 'fixture-token' not in old
    monkeypatch.setenv('A2A_PEER_TOKENS', 'fixture-peer:new-fixture-token')
    new = A2AAdapter(PlatformConfig(enabled=True))
    assert GatewayRunner._adapter_credential_fingerprint(new) != old


@pytest.mark.parametrize('key,value', [('A2A_HOST','127.0.0.1'),
                                      ('A2A_ALLOW_ALL_USERS','true'),
                                      ('A2A_PEER_TOKENS','other-peer:fixture-token')])
def test_restarted_connector_cannot_reuse_different_auth_policy(connector, monkeypatch, key, value):
    original = GatewayRunner._adapter_credential_fingerprint(connector)
    monkeypatch.setenv(key,value)
    replacement = A2AAdapter(PlatformConfig(enabled=True))
    assert GatewayRunner._adapter_credential_fingerprint(replacement) != original


@pytest.mark.anyio
async def test_real_native_admission_accepts_peer_but_not_forged_or_revoked(connector, monkeypatch):
    runner = GatewayRunner()
    await initialize_session_authority(runner, profile_id='default', instance_id='a2a-fixture')
    connector.gateway_runner = runner
    runner.adapters[connector.platform] = connector
    home = Path(os.environ['HERMES_HOME'])
    register_transport_home(runner, None, home)
    captured = []
    ready = asyncio.Event()

    async def collect(event):
        captured.append(event)
        ready.set()

    connector.handle_message = collect
    connector._loop = asyncio.get_running_loop()
    connector._message_handler = collect
    params = {'message': {'role': 'user', 'parts': [{'kind': 'text', 'text': 'SYNTHETIC_READY'}]}}
    pending = await asyncio.to_thread(connector._prepare_task, params, 'fixture-peer')
    await asyncio.wait_for(ready.wait(), timeout=3)
    assert pending[0] is None and captured
    event = captured[0]
    assert event.source.role_authorized is True
    with native_callback(runner, event, home):
        payload = await prepare_native(runner, event)
    assert payload['native_text_v1']['reauthorize'] == 'roles'
    # Recreate the connector as on restart: unchanged policy retains its binding.
    replacement = A2AAdapter(PlatformConfig(enabled=True))
    runner.adapters[connector.platform] = replacement
    provenance = payload['native_text_v1']['provenance']
    restored = copy(event.source)
    restore_provenance(runner, restored, provenance)
    assert await reauthorize_roles(runner, restored, provenance) is True
    runner.adapters[connector.platform] = connector
    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        await prepare_native(runner, event)  # A serialized role flag cannot mint callback provenance.
    monkeypatch.setenv('A2A_TRUSTED_PEERS', 'other-peer')
    with native_callback(runner, event, home):
        with pytest.raises(RuntimeStoreError, match='permission_denied'):
            await prepare_native(runner, event)
    replacement = A2AAdapter(PlatformConfig(enabled=True))
    runner.adapters[connector.platform] = replacement
    with pytest.raises(RuntimeStoreError, match='profile_mismatch'):
        restore_provenance(runner, event.source, provenance)


@pytest.mark.anyio
async def test_reauthorization_never_borrows_other_profile_credentials(connector, tmp_path):
    import agent.secret_scope as scopes
    import hermes_constants
    home = tmp_path / 'other-profile'
    home.mkdir()
    (home / '.env').write_text('A2A_PEER_TOKENS=other-peer:other-fixture\n'
                              'A2A_TRUSTED_PEERS=other-peer\nA2A_HOST=0.0.0.0\n')
    prior = scopes._MULTIPLEX_ACTIVE
    scopes.set_multiplex_active(True)
    home_token = hermes_constants.set_hermes_home_override(str(home))
    token = scopes.set_secret_scope(scopes.build_profile_secret_scope(home), profile_home=str(home))
    try:
        assert await connector.reauthorize_native_roles(source(connector)) is False
    finally:
        scopes.reset_secret_scope(token)
        hermes_constants.reset_hermes_home_override(home_token)
        scopes.set_multiplex_active(prior)
