"""Идемпотентность вебхука Fragment: заявка снимается, если обработка не дошла до 200."""

import asyncio

import pytest

from django.http import HttpRequest, HttpResponse

import core.integrations.webhook_utils as wh
from core.integrations.webhook_utils import (
    ServicesNames,
    release_fragment_idempotency_key,
    validate_fragment_idempotency_key,
)


class _FakeRedis:
    """Минимум команд, которые нужны заявке идемпотентности."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return int(self.store.pop(key, None) is not None)


def _request(idem_key: str | None = "idem-1") -> HttpRequest:
    request = HttpRequest()
    if idem_key is not None:
        request.META["HTTP_X_IDEMPOTENCY_KEY"] = idem_key
    return request


@pytest.fixture
def redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    fake = _FakeRedis()
    monkeypatch.setattr(wh, "get_async_redis_client", lambda: fake)
    return fake


def test_repeat_delivery_is_dropped_while_claim_stands(redis: _FakeRedis):
    assert asyncio.run(validate_fragment_idempotency_key(_request())) is None

    repeat = asyncio.run(validate_fragment_idempotency_key(_request()))
    assert isinstance(repeat, HttpResponse)
    assert repeat.status_code == 200


def test_released_claim_lets_the_retry_through(redis: _FakeRedis):
    _ = asyncio.run(validate_fragment_idempotency_key(_request()))
    asyncio.run(release_fragment_idempotency_key(_request(), ServicesNames.FRAGMENT))

    # Ретрай приходит с тем же ключом - он обязан обработаться, а не получить молчаливый 200.
    assert asyncio.run(validate_fragment_idempotency_key(_request())) is None
    assert redis.store != {}


def test_claim_is_stored_with_ttl_in_one_command(redis: _FakeRedis):
    calls: list[dict[str, object]] = []

    async def _set(key: str, value: str, ex: int | None = None, nx: bool = False):
        calls.append({"ex": ex, "nx": nx})
        return True

    redis.set = _set  # pyright: ignore[reportAttributeAccessIssue]
    _ = asyncio.run(validate_fragment_idempotency_key(_request()))

    assert calls == [{"ex": 172800, "nx": True}]


def test_release_ignores_other_services_and_missing_key(redis: _FakeRedis):
    _ = asyncio.run(validate_fragment_idempotency_key(_request()))
    claimed = dict(redis.store)

    asyncio.run(release_fragment_idempotency_key(_request(), ServicesNames.PAYPEAR))
    asyncio.run(release_fragment_idempotency_key(_request(None), ServicesNames.FRAGMENT))

    assert redis.store == claimed


def test_webhook_releases_claim_when_handler_answers_non_200(
        redis: _FakeRedis, monkeypatch: pytest.MonkeyPatch
):
    import core.views as views

    async def _granted(*_: object) -> None:
        return None

    async def _handler(*_: object) -> HttpResponse:
        return HttpResponse(status=429)

    monkeypatch.setattr(views, "access_granted_or_http_response", _granted)
    monkeypatch.setattr(views, "_handle_webhook", _handler)

    request = _request()
    _ = asyncio.run(validate_fragment_idempotency_key(request))

    response = asyncio.run(views._process_webhook(request, ServicesNames.FRAGMENT))

    assert response.status_code == 429
    assert redis.store == {}, "после 429 заявка должна быть снята, иначе ретрай потеряется"


def test_webhook_releases_claim_when_handler_raises(
        redis: _FakeRedis, monkeypatch: pytest.MonkeyPatch
):
    import core.views as views

    async def _granted(*_: object) -> None:
        return None

    async def _handler(*_: object) -> HttpResponse:
        raise RuntimeError("boom")

    monkeypatch.setattr(views, "access_granted_or_http_response", _granted)
    monkeypatch.setattr(views, "_handle_webhook", _handler)

    request = _request()
    _ = asyncio.run(validate_fragment_idempotency_key(request))

    with pytest.raises(RuntimeError):
        _ = asyncio.run(views._process_webhook(request, ServicesNames.FRAGMENT))

    assert redis.store == {}


def test_webhook_keeps_claim_on_success(redis: _FakeRedis, monkeypatch: pytest.MonkeyPatch):
    import core.views as views

    async def _granted(*_: object) -> None:
        return None

    async def _handler(*_: object) -> HttpResponse:
        return HttpResponse(status=200)

    monkeypatch.setattr(views, "access_granted_or_http_response", _granted)
    monkeypatch.setattr(views, "_handle_webhook", _handler)

    request = _request()
    _ = asyncio.run(validate_fragment_idempotency_key(request))

    response = asyncio.run(views._process_webhook(request, ServicesNames.FRAGMENT))

    assert response.status_code == 200
    assert redis.store != {}, "обработанное уведомление не должно приниматься повторно"
