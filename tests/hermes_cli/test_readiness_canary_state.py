"""Windows BOM state must retain canary alert deduplication."""
import json

from hermes_cli import computer_use_readiness_canary as canary


def test_bom_state_preserves_existing_alert_window(tmp_path, monkeypatch):
    path = tmp_path / 'state' / 'computer_use_readiness_canary.json'
    path.parent.mkdir()
    path.write_text(json.dumps({'results': [{'profile': 'default', 'ok': False,
                                           'alerted_at': 1000}]}), encoding='utf-8-sig')
    monkeypatch.setattr(canary, '_profiles', lambda root: [('default', root)])
    monkeypatch.setattr(canary, '_probe', lambda *args, **kwargs: {'profile': 'default', 'ok': False})
    events = []
    monkeypatch.setattr(canary, '_event_log', lambda *args: events.append(args))
    result = canary.run(root=tmp_path, now=1001)
    assert result['events'] == events == []
    assert result['results'][0]['alerted_at'] == 1000
