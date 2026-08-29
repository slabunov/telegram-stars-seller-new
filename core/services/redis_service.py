from __future__ import annotations

import asyncio
import random
import logging
from uuid import UUID
from dataclasses import dataclass
from typing import Protocol, TypedDict, cast
from collections.abc import Sequence

from django.conf import settings

from redis import Redis as sync_Redis, from_url as sync_from_url  # noqa
from redis.lock import Lock as sync_Lock  # noqa
from redis.asyncio import Redis as async_Redis, from_url as async_from_url  # noqa
from redis.asyncio.lock import Lock as async_Lock  # noqa

from celery import Task

from general.utils import json_dumps, json_loads

from bot.notifications.broadcast import process_preview, process_broadcast
from bot.utils.type_aliases import DefaultApplication


logger = logging.getLogger(__name__)


_URL = cast(str, settings.CELERY_BROKER_URL)
_CONNECT_TIMEOUT = 15.0
_TIMEOUT = 25.0
_KEEPALIVE = True
_DECODE_RESPONSES = True


redis_client: sync_Redis = sync_from_url(
    _URL,
    socket_connect_timeout=_CONNECT_TIMEOUT,
    socket_timeout=_TIMEOUT,
    socket_keepalive=_KEEPALIVE,
    decode_responses=_DECODE_RESPONSES
)
_async_redis_client: async_Redis | None = None


def get_async_redis_client() -> async_Redis:
    global _async_redis_client
    if _async_redis_client is None:
        _async_redis_client = async_from_url(
            _URL,
            socket_connect_timeout=_CONNECT_TIMEOUT,
            socket_timeout=_TIMEOUT,
            socket_keepalive=_KEEPALIVE,
            decode_responses=_DECODE_RESPONSES
        )
    return _async_redis_client


async def close_async_redis_client() -> None:
    global _async_redis_client
    if _async_redis_client is not None:
        await _async_redis_client.close()
        _async_redis_client = None


LOCK_PAYMENT = "lock_payment"
MESSAGE_POLLING = "message_polling"
LATEST_STATUS = "latest_status"
LOCK_LATEST_STATUS = f"lock_{LATEST_STATUS}"

LOCK_FRAGMENT = "lock_fragment"
FRAGMENT_IDEM_KEY = "fragment_idem_key"

LOCK_PROMO_INPUT_PROCESSING = "lock_promo_input_processing"

LOCK_ORDER_CONFIRM = "lock_order_confirm"


_lua_get_and_del = """
local val = redis.call('GET', KEYS[1])
if val and val ~= '' then
    redis.call('DEL', KEYS[1])
    return val
else
    return nil
end
"""

class _GetAndDel(Protocol):
    def __call__(self, keys: Sequence[str]) -> str | None: ...

get_and_del: _GetAndDel = redis_client.register_script(_lua_get_and_del)


def get_lock_latest_status(service_name: str, transaction_id: str | UUID) -> str:
    return f"{LOCK_LATEST_STATUS}:{service_name}:{transaction_id}"


def get_lock_payment_transaction(transaction_id: str | UUID) -> str:
    return f"{LOCK_PAYMENT}:{transaction_id}"


def get_lock_payment_message_polling(transaction_id: str | UUID) -> str:
    return f"{LOCK_PAYMENT}:{transaction_id}:{MESSAGE_POLLING}"


def get_lock_fragment_transaction(transaction_id: str | UUID) -> str:
    return f"{LOCK_FRAGMENT}:{transaction_id}"


def get_lock_promo_input_processing() -> str:
    return f"{LOCK_PROMO_INPUT_PROCESSING}"


def get_lock_order_confirm(telegram_id: int) -> str:
    return f"{LOCK_ORDER_CONFIRM}:{telegram_id}"


def get_key_latest_status(service_name: str, transaction_id: str | UUID) -> str:
    return f"{LATEST_STATUS}:{service_name}:{transaction_id}"


def get_key_fragment_idem(idem_key: str) -> str:
    return f"{FRAGMENT_IDEM_KEY}:{idem_key}"


def sync_save_status_by_key(
        service_name: str, transaction_id: str | UUID, status: str,
        *,
        if_not_exists: bool = False
) -> bool:
    """
    Если `if_not_exists` равен `True`, то будет `redis_client.set(nx=True)`, что означает сохранить статус только если
    такого ключа не существует.
    """
    key = get_key_latest_status(service_name, transaction_id)
    return cast(bool, redis_client.set(key, status, ex=172800, nx=if_not_exists))  # 48 часов  # noqa


async def async_save_status_by_key(
        service_name: str, transaction_id: str | UUID, status: str,
        *,
        if_not_exists: bool = False
) -> bool:
    """
    Если `if_not_exists` равен `True`, то будет `redis_client.set(nx=True)`, что означает сохранить статус только если
    такого ключа не существует.
    """
    async_redis_client = get_async_redis_client()
    key = get_key_latest_status(service_name, transaction_id)
    return cast(bool, await async_redis_client.set(key, status, ex=172800, nx=if_not_exists))  # 48 часов  # noqa


def get_and_del_by_key(service_name: str, transaction_id: str | UUID) -> str | None:
    key = get_key_latest_status(service_name, transaction_id)
    return get_and_del(keys=[key])


def sync_acquire_lock(
        lock_name: str,
        timeout: float = 180.0,
        blocking: bool = True, blocking_timeout: float = 10.0
) -> sync_Lock | None:
    lock = cast(sync_Lock, redis_client.lock(
        lock_name,
        timeout=timeout,
        blocking=blocking, blocking_timeout=blocking_timeout
    ))

    if not lock.acquire():
        return None

    return lock


async def async_acquire_lock(
        lock_name: str,
        timeout: float = 180.0,
        blocking: bool = True, blocking_timeout: float = 10.0
) -> async_Lock | None:
    lock = get_async_redis_client().lock(
        lock_name,
        timeout=timeout,
        blocking=blocking, blocking_timeout=blocking_timeout
    )

    if not await lock.acquire():
        return None

    return lock


def sync_get_lock_or_retry[**P, R](
        celery_task: Task[P,R],
        lock_name: str,
        base_delay: float = 5.0, max_jitter: float = 3.0,
        timeout: float = 180.0, blocking: bool = True, blocking_timeout: float = 10.0
) -> sync_Lock:
    lock = sync_acquire_lock(lock_name, timeout, blocking, blocking_timeout)

    if lock is None:
        jitter = random.uniform(0.0, abs(max_jitter))
        raise celery_task.retry(countdown=base_delay + jitter, max_retries=None)

    return lock


BOT_BROADCAST_CHANNEL = "bot_broadcast_channel"


@dataclass(frozen=True, slots=True)
class BroadcastPublishDTO:
    action: str
    broadcast_id: int


class PubSubMessage(TypedDict):
    type: str
    pattern: str | None
    channel: str
    data: str | int


def publish_broadcast_task(action: str, broadcast_id: int) -> None:
    broadcast_publish = BroadcastPublishDTO(action, broadcast_id)
    payload = json_dumps(broadcast_publish)
    _ = redis_client.publish(BOT_BROADCAST_CHANNEL, payload)  # pyright: ignore[reportUnknownMemberType]


async def listen_redis_for_broadcasts(app: DefaultApplication) -> None:
    """Слушает Pub/Sub канал Redis и запускает рассылки внутри цикла бота."""

    while not app.running:
        if app.bot_data.get("stop_redis", False):
            logger.warning("Бот: был запрос на остановку прослушивания рассылок и не дождался старта бота!")
            return

        await asyncio.sleep(0.1)

    async_redis_client = get_async_redis_client()
    pubsub = async_redis_client.pubsub()  # pyright: ignore[reportUnknownMemberType]

    await pubsub.subscribe(BOT_BROADCAST_CHANNEL)
    logger.info(f"Начато прослушивание канала {BOT_BROADCAST_CHANNEL}")

    try:
        while not app.bot_data.get("stop_redis", False):
            message = cast(
                PubSubMessage | None, await pubsub.get_message(ignore_subscribe_messages=True, timeout=3.0)
            )

            if message is not None and message["type"] == "message":
                msg_data = cast(str, message["data"])  # noqa
                data = json_loads(msg_data, BroadcastPublishDTO)

                action = data.action
                if action == "preview":
                    _ = app.create_task(process_preview(app.bot, data.broadcast_id))
                elif action == "broadcast":
                    _ = app.create_task(process_broadcast(app.bot, data.broadcast_id))

    except Exception as exc:
        logger.exception(f"Возникло исключение во время прослушивания канала {BOT_BROADCAST_CHANNEL}: {exc}")

    finally:
        await pubsub.close()
        logger.info(f"Бот: прослушивание Redis по каналу {BOT_BROADCAST_CHANNEL} остановлено!")


class DecodingRedisDataError(Exception):
    """Ошибка декодирования данных из редиса."""
