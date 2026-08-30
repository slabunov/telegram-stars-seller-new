"""
Опрос статусов заказов fragment-api для транзакций, которые уже уехали во Fragment,
но так и не получили финальный статус.

О завершении заказа Fragment сообщает только вебхуком на `response_url` (см.
`FragmentClient.build_response_url`). Если этот вебхук не доходит - нет публичного URL,
сеть, ошибка на стороне Fragment - транзакция навсегда зависает в `SEND_CREATED`:
пользователь остаётся с сообщением "Заказ обрабатывается...", а заказ не попадает
в историю покупок, потому что там показываются только `SUCCESS`.

Задача - зеркало `poll_pending_paypear_payments_task`: раз в `FRAGMENT_POLL_SECONDS`
celery-beat забирает висящие заказы и дочитывает их статус через `GET /order/{id}/`,
отдавая результат в тот же конвейер, что и вебхук.
"""

from __future__ import annotations

import time
import logging
from uuid import UUID
from datetime import timedelta
from typing import ParamSpec, TypeVar

from asgiref.sync import async_to_sync
from celery import shared_task

from django.conf import settings
from django.utils import timezone

from core.domain.enums import TransactionStatus
from core.integrations.fragment.enums import FragmentStatus
from core.tasks.utils import Task
from core.models import FragmentTransaction, Transaction


logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

_LOCK_NAME = "lock_poll_fragment"
_ACTED_PREFIX = "fragment:poll_acted:"     # маркер "переход уже поставлен в очередь"
_ACTED_TTL = 30                            # сек - за это время конвейер уводит статус из ожидания
_MAX_PER_TICK = 50                         # предохранитель от лимитов fragment-api

# Статусы транзакции, из которых её ещё может вытащить ответ Fragment
WAITING_STATUSES = (TransactionStatus.SEND_CREATED, TransactionStatus.IN_DOUBT)

# Статусы заказа Fragment, по которым можно закрывать транзакцию
_FINAL_FRAGMENT_STATUSES = (FragmentStatus.COMPLETED, FragmentStatus.FAILED)


def decide_next_status(raw_status: str, age: timedelta, doubt_after: timedelta) -> str | None:
    """
    Какой статус транзакции соответствует ответу Fragment.

    Returns:
        `None`, если заказ ещё в работе и статус транзакции трогать рано; иначе новый статус
    """
    if raw_status in _FINAL_FRAGMENT_STATUSES:
        return str(FragmentStatus.transform_into_internal_status_or_keep_original(raw_status))

    if age > doubt_after:
        # Заказ висит слишком долго. Уводим пользователя из "Заказ обрабатывается..."
        # к поддержке; из IN_DOUBT переход в SUCCESS/FAILED всё ещё разрешён,
        # так что опрос доведёт заказ до конца, если Fragment его всё-таки закроет.
        return str(TransactionStatus.IN_DOUBT)

    return None


def push_fragment_status(
        transaction_id: UUID,
        fragment_tx_id: UUID,
        raw_status: str,
        internal_status: str | None
) -> None:
    """
    Кладёт свежий статус в те же ключи Redis и те же задачи, что и вебхук
    (`core.views._process_webhook`).

    `internal_status` равен `None`, когда нужно освежить только `FragmentTransaction`,
    не трогая статус самой транзакции.
    """
    from core.integrations.fragment.tasks import update_fragment_tx_task
    from core.integrations.platega.tasks import update_transaction_status_task
    from core.integrations.webhook_utils import ServicesNames
    from core.services.redis_service import sync_save_status_by_key

    _ = sync_save_status_by_key(ServicesNames.FRAGMENT__FROM_POLL, transaction_id, raw_status)
    _ = update_fragment_tx_task.apply_async(
        args=(str(fragment_tx_id), str(transaction_id)),
        kwargs={"started_at": None},
    )

    if internal_status is None:
        return

    _ = sync_save_status_by_key(ServicesNames.FRAGMENT, transaction_id, internal_status)
    _ = update_transaction_status_task.apply_async(
        args=(str(transaction_id), None, ""),
        kwargs={"started_at": None},
    )


async def _latest_fragment_tx_by_transaction(
        transaction_ids: list[UUID]
) -> dict[UUID, FragmentTransaction]:
    """
    Последний заказ Fragment для каждой транзакции. Обычно он ровно один, но повторная
    отправка звёзд теоретически может оставить несколько - тогда актуален самый свежий.
    """
    query = (
        FragmentTransaction.objects
        .filter(id_from_payment_api__in=transaction_ids)
        .order_by("created_at")
    )
    return {row.id_from_payment_api: row async for row in query}


async def _poll_unfinished_fragment_orders() -> str:
    from core.integrations.fragment.client import FragmentClient
    from core.integrations.webhook_utils import ServicesNames
    from core.ioc import get_container
    from core.services.redis_service import (
        async_acquire_lock, get_lock_latest_status, redis_client
    )

    now = timezone.now()
    max_age = timedelta(hours=settings.FRAGMENT_POLL_MAX_AGE_HOURS)
    doubt_after = timedelta(minutes=settings.FRAGMENT_POLL_DOUBT_AFTER_MINUTES)

    query = (
        Transaction.objects
        .filter(status__in=WAITING_STATUSES, created_at__gte=now - max_age)
        .order_by("created_at")
    )[:_MAX_PER_TICK]
    waiting = [t async for t in query]
    if not waiting:
        return "no unfinished fragment orders"

    fragment_txs = await _latest_fragment_tx_by_transaction([t.id for t in waiting])
    client = await get_container().get(FragmentClient)

    checked = 0
    advanced = 0
    for txn in waiting:
        fragment_tx = fragment_txs.get(txn.id)
        if fragment_tx is None:
            # Заказа во Fragment нет - опрашивать нечего, это отдельная поломка
            # (звёзды не отправились), её разбирает админ.
            continue

        if redis_client.exists(f"{_ACTED_PREFIX}{txn.id}"):
            continue

        checked += 1
        try:
            order = await client.get_order(fragment_tx.fragment_id, timeout=10.0, connect=5.0)
            if order is None:
                continue

            raw_status = str(order.get("status", ""))
            if not raw_status:
                continue

            internal_status = decide_next_status(
                raw_status, timezone.now() - txn.created_at, doubt_after
            )

            if internal_status is None:
                # CREATED / PENDING / BLOCKCHAIN_SENT - заказ ещё в работе. Статус транзакции
                # не трогаем (иначе пользователь раньше времени увидит "ПОД СОМНЕНИЕМ"),
                # но саму запись FragmentTransaction освежаем.
                if raw_status != fragment_tx.status:
                    push_fragment_status(txn.id, fragment_tx.fragment_id, raw_status, None)
                continue

            if internal_status == txn.status:
                continue

            # Тот же замок, что берёт вебхук - чтобы не разъехаться с ним, если он всё-таки дошёл
            lock = await async_acquire_lock(
                get_lock_latest_status(ServicesNames.FRAGMENT, txn.id),
                timeout=45.0,
                blocking=False, blocking_timeout=0.0
            )
            if lock is None:
                continue

            try:
                push_fragment_status(txn.id, fragment_tx.fragment_id, raw_status, internal_status)
                _ = redis_client.set(f"{_ACTED_PREFIX}{txn.id}", "1", ex=_ACTED_TTL)
                advanced += 1
            finally:
                try:
                    await lock.release()
                except Exception as exc:
                    logger.warning(f"Fragment poll: lock release failed for {txn.id}: {exc}")

        except Exception as exc:
            logger.warning(f"Fragment poll: {txn.id} -> {exc.__class__.__name__}: {exc}")

    return f"Fragment poll: checked {checked}, advanced {advanced}"


@shared_task(bind=True, ignore_result=True)
def poll_unfinished_fragment_orders_task(self: Task[P, R], *, started_at: float | None = None) -> str:
    from core.services.redis_service import sync_acquire_lock

    _ = started_at or time.time()

    lock = sync_acquire_lock(_LOCK_NAME, timeout=120.0, blocking=False, blocking_timeout=0.0)
    if lock is None:
        return "Fragment poll already running"

    try:
        return async_to_sync(_poll_unfinished_fragment_orders)()
    finally:
        try:
            lock.release()
        except Exception as exc:
            logger.warning(f"Fragment poll: lock release failed: {exc}")
