"""Замок против дублей при создании заказа (двойной тап / ретрай Telegram по кнопке подтверждения)."""

import asyncio
from unittest.mock import AsyncMock

import bot.handlers.order as order_mod
from bot.states import BotConversationState


class _FakeLock:
    def __init__(self) -> None:
        self.released = False

    async def release(self) -> None:
        self.released = True


def _fake_update() -> AsyncMock:
    update = AsyncMock()
    update.effective_user.id = 123
    return update


def test_duplicate_confirm_is_rejected_without_creating_order(monkeypatch):
    monkeypatch.setattr(order_mod, "async_acquire_lock", AsyncMock(return_value=None))
    helper = AsyncMock()
    monkeypatch.setattr(order_mod, "_handle_order_confirmed_helper", helper)

    update = _fake_update()
    result = asyncio.run(order_mod._create_order_once(update, AsyncMock()))

    assert result == BotConversationState.ORDER_CONFIRMATION
    helper.assert_not_awaited()
    update.callback_query.answer.assert_awaited_once_with(
        text=order_mod.ORDER_CONFIRM_BUSY_MESSAGE, show_alert=True
    )


def test_winning_confirm_runs_once_and_releases_lock(monkeypatch):
    lock = _FakeLock()
    monkeypatch.setattr(order_mod, "async_acquire_lock", AsyncMock(return_value=lock))
    helper = AsyncMock(return_value=BotConversationState.ORDER_CONFIRMED)
    monkeypatch.setattr(order_mod, "_handle_order_confirmed_helper", helper)

    result = asyncio.run(order_mod._create_order_once(_fake_update(), AsyncMock()))

    assert result == BotConversationState.ORDER_CONFIRMED
    helper.assert_awaited_once()
    assert lock.released is True


def test_lock_is_released_even_if_order_creation_raises(monkeypatch):
    lock = _FakeLock()
    monkeypatch.setattr(order_mod, "async_acquire_lock", AsyncMock(return_value=lock))
    monkeypatch.setattr(
        order_mod, "_handle_order_confirmed_helper", AsyncMock(side_effect=RuntimeError("boom"))
    )

    try:
        asyncio.run(order_mod._create_order_once(_fake_update(), AsyncMock()))
    except RuntimeError:
        pass
    else:
        raise AssertionError("RuntimeError должен пробрасываться в error_handler")

    assert lock.released is True
