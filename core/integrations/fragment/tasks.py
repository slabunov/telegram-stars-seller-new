from __future__ import annotations

import time
import logging
from uuid import UUID
from typing import ParamSpec, TypeVar

from asgiref.sync import async_to_sync
from celery import shared_task

from core.integrations.fragment.webhook_workflow import update_fragment_transaction_workflow
from core.integrations.webhook_utils import ServicesNames
from core.services.redis_service import (
    get_and_del_by_key, sync_save_status_by_key,
    sync_get_lock_or_retry, get_lock_fragment_transaction
)
from core.tasks import Task


logger = logging.getLogger(__name__)


P = ParamSpec("P")
R = TypeVar("R")


@shared_task(bind=True, max_retries=100)
def update_fragment_tx_task(
        self: Task[P,R],
        fragment_tx_id: str,
        transaction_id: str,
        *,
        started_at: float | None
) -> str:
    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        status_from_webhook = get_and_del_by_key(
            ServicesNames.FRAGMENT__FROM_WEBHOOK, transaction_id
        )
        status_from_poll = get_and_del_by_key(
            ServicesNames.FRAGMENT__FROM_POLL, transaction_id
        )
        status_from_creation = get_and_del_by_key(
            ServicesNames.FRAGMENT__FROM_CREATION, transaction_id
        )

        # Вебхук и опрос одинаково авторитетны, но вебхук приходит раньше, поэтому он в приоритете.
        # Статус из момента создания заказа - самый старый, он идёт последним.
        if status_from_webhook is not None:
            new_status = status_from_webhook

        elif status_from_poll is not None:
            new_status = status_from_poll

        elif status_from_creation is not None:
            new_status = status_from_creation

        else:
            return (
                f"New status for fragment transaction {fragment_tx_id} (platega {transaction_id}) was already processed"
            )

        is_success: bool = False
        try:
            is_success, msg = await update_fragment_transaction_workflow(
                self, UUID(fragment_tx_id), UUID(transaction_id),
                new_status,
                started_at=started_at
            )
            return msg

        finally:
            if not is_success:
                if new_status == status_from_webhook:
                    _ = sync_save_status_by_key(
                        ServicesNames.FRAGMENT__FROM_WEBHOOK, transaction_id, new_status,
                        if_not_exists=True
                    )

                elif new_status == status_from_poll:
                    _ = sync_save_status_by_key(
                        ServicesNames.FRAGMENT__FROM_POLL, transaction_id, new_status,
                        if_not_exists=True
                    )

                elif new_status == status_from_creation:
                    _ = sync_save_status_by_key(
                        ServicesNames.FRAGMENT__FROM_CREATION, transaction_id, new_status,
                        if_not_exists=True
                    )

    lock = sync_get_lock_or_retry(self, get_lock_fragment_transaction(transaction_id))

    try:
        return async_to_sync(critical_section)()
    finally:
        lock.release()
