"""
Опрос статусов платежей PayPear, когда нет публичного URL для webhook-ов
(PAYPEAR_USE_WEBHOOK=False). Раз в PAYPEAR_POLL_SECONDS celery-beat дёргает эту
задачу; она находит висящие PENDING-транзакции PayPear и подтягивает их статус
через GET /payment/order/{order_id}/, передавая изменения в тот же конвейер,
что и webhook.
"""

from __future__ import annotations

import time
import logging
from datetime import timedelta
from typing import ParamSpec, TypeVar

from asgiref.sync import async_to_sync
from celery import shared_task

from django.utils import timezone

from core.domain.enums import TransactionStatus
from core.tasks.utils import Task
from core.models import Transaction


logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

_LOCK_NAME = "lock_poll_paypear"
_MAX_AGE = timedelta(minutes=40)          # платёж PayPear живёт 30 мин; дальше он точно EXPIRED
_ACTED_PREFIX = "paypear:poll_acted:"     # маркер "переход уже поставлен в очередь"
_ACTED_TTL = 30                           # сек - за это время конвейер уводит статус из PENDING


async def _poll_pending_paypear() -> str:
    from core.integrations.paypear.client import PayPearClient
    from core.integrations.platega.tasks import update_transaction_status_task
    from core.integrations.webhook_utils import ServicesNames, transform_into_internal_status_or_keep_original
    from core.ioc import get_container
    from core.services.redis_service import redis_client, sync_save_status_by_key

    now = timezone.now()
    query = (
        Transaction.objects
        .filter(
            status=TransactionStatus.PENDING,
            metadata_info__payment_method__startswith="PayPear",
            created_at__gte=now - _MAX_AGE,
        )
        .order_by("created_at")
    )
    pending = [t async for t in query]
    if not pending:
        return "no pending PayPear payments"

    client = await get_container().get(PayPearClient)

    checked = 0
    advanced = 0
    for txn in pending:
        if redis_client.exists(f"{_ACTED_PREFIX}{txn.id}"):
            continue

        checked += 1
        try:
            payment_object = await client.get_payment_info(txn.id, timeout=10.0, connect=5.0)
            if payment_object is None:
                continue

            raw_status = payment_object.get("status", "")
            new_status = transform_into_internal_status_or_keep_original(raw_status, ServicesNames.PAYPEAR)

            if not raw_status or new_status == TransactionStatus.PENDING:
                continue

            _ = sync_save_status_by_key(ServicesNames.PAYPEAR, txn.id, str(new_status))
            _ = update_transaction_status_task.apply_async(
                args=(str(txn.id), None, ""),
                kwargs={"started_at": None},
            )
            _ = redis_client.set(f"{_ACTED_PREFIX}{txn.id}", "1", ex=_ACTED_TTL)
            advanced += 1

        except Exception as exc:
            logger.warning(f"PayPear poll: {txn.id} -> {exc.__class__.__name__}: {exc}")

    return f"PayPear poll: checked {checked}, advanced {advanced}"


@shared_task(bind=True, ignore_result=True)
def poll_pending_paypear_payments_task(self: Task[P, R], *, started_at: float | None = None) -> str:
    from core.services.redis_service import sync_acquire_lock

    _ = started_at or time.time()

    lock = sync_acquire_lock(_LOCK_NAME, timeout=30.0, blocking=False, blocking_timeout=0.0)
    if lock is None:
        return "PayPear poll already running"

    try:
        return async_to_sync(_poll_pending_paypear)()
    finally:
        try:
            lock.release()
        except Exception as exc:
            logger.warning(f"PayPear poll: lock release failed: {exc}")
