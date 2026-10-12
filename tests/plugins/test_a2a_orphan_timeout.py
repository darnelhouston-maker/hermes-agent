from plugins.platforms.a2a import adapter


def test_orphan_timeout_tracks_reply_window(monkeypatch):
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "7200")
    assert adapter._reply_timeout() == 7200
    assert adapter._orphan_timeout() == 7200


def test_orphan_timeout_keeps_safe_bounds(monkeypatch):
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "10")
    assert adapter._orphan_timeout() == 300
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "999999")
    assert adapter._orphan_timeout() == 86400
