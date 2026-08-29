from __future__ import annotations

import time
import logging
from uuid import UUID
from typing import cast, ParamSpec, TypeVar

from celery import shared_task

from asgiref.sync import async_to_sync

from telegram.constants import ParseMode

from core.domain.enums import TransactionStatus, is_change_status_allowed
from core.integrations.fragment.tasks import update_fragment_tx_task
from core.integrations.platega.schemas import PaymentPayloadDict
from core.integrations.platega.webhook_utils import (
    safe_create_transaction_with_retries,
    safe_get_transaction_with_retries,
    safe_set_status_for_transaction_obj_with_retries,
    safe_set_status_for_transaction_id_with_retries, create_fragment_transaction_if_not_sent_with_retries,
    safe_update_transaction_payload_with_retries
)
from core.integrations.platega.webhook_workflow import (
    update_order_message_workflow
)
from core.integrations.webhook_utils import (
    PAYMENT_SERVICE_NAMES, ServicesNames, transform_into_internal_status_or_keep_original
)
from core.services.redis_service import (
    sync_acquire_lock, sync_get_lock_or_retry,
    get_lock_payment_transaction, get_lock_payment_message_polling,
    get_and_del_by_key, sync_save_status_by_key,
)
from core.tasks import Task


logger = logging.getLogger(__name__)
cleanup_logger = logging.getLogger("cleanup_audit")


P = ParamSpec("P")
R = TypeVar("R")


@shared_task(bind=True, acks_late=True, max_retries=100)
def update_transaction_status_task(
        self: Task[P,R],
        transaction_id: str,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float | None
) -> str:
    """
    Эта задача выполнится в фоне воркером Celery.

    В аргументах должны быть объекты, которые могут быть сериализуемые в JSON.
    """

    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        payment_service_name: str | None = None
        payment_status: str | None = None
        for candidate in PAYMENT_SERVICE_NAMES:
            candidate_status = get_and_del_by_key(candidate, transaction_id)
            if isinstance(candidate_status, bytes):
                candidate_status = candidate_status.decode("utf-8")
            if candidate_status is not None:
                payment_service_name = candidate
                payment_status = candidate_status
                break

        fragment_status = get_and_del_by_key(ServicesNames.FRAGMENT, transaction_id)
        if isinstance(fragment_status, bytes):
            fragment_status = fragment_status.decode("utf-8")

        if payment_status is not None and fragment_status is not None:
            if payment_status == TransactionStatus.CHARGEBACKED:
                new_status = payment_status

            else:
                new_status = fragment_status

        elif payment_status is not None:
            new_status = payment_status

        elif fragment_status is not None:
            new_status = fragment_status

        else:
            return f"new status for transaction {transaction_id} was already processed"

        is_success: bool = False
        try:
            is_success, msg = await update_transaction_status_workflow(
                self,
                UUID(transaction_id),
                new_status,
                parsed_payload,
                payment_method,
                started_at=started_at
            )
            return msg

        finally:
            if not is_success:
                if payment_service_name is not None and new_status == payment_status:
                    _ = sync_save_status_by_key(
                        payment_service_name, transaction_id, new_status,
                        if_not_exists=True
                    )

                elif new_status == fragment_status:
                    _ = sync_save_status_by_key(
                        ServicesNames.FRAGMENT, transaction_id, new_status,
                        if_not_exists=True
                    )

    lock = sync_get_lock_or_retry(self, get_lock_payment_transaction(transaction_id))

    try:
        return async_to_sync(critical_section)()
    finally:
        lock.release()


@shared_task(bind=True, acks_late=True, max_retries=100)
def update_order_message_task(
        self: Task[P,R],
        parse_mode: str,
        user_id: int,
        message_id: int,
        transaction_id: str,
        amount_stars: int,
        price: str,
        target_username: str,
        pay_url: str,
        promo_name: str, promo_discount: str | None,
        *,
        started_at: float | None
) -> str:
    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        return await update_order_message_workflow(
            self,
            parse_mode,
            user_id,
            message_id,
            UUID(transaction_id),
            amount_stars,
            price,
            target_username,
            pay_url,
            promo_name, promo_discount,
            started_at=started_at
        )

    lock = sync_acquire_lock(
        get_lock_payment_message_polling(transaction_id),
        blocking=False,
        blocking_timeout=0.0
    )
    if lock is None:
        # Если мы не смогли получить замок, значит мы дубликат - можно завершаться
        return f"message already updating for transaction {transaction_id}"

    try:
        return async_to_sync(critical_section)()
    finally:
        lock.release()


@shared_task(bind=True, acks_late=True, max_retries=100)
def update_transaction_payload_task(
        self: Task[P,R],
        transaction_id: str,
        new_payload: dict[str, object],
        *,
        started_at: float | None
) -> str:
    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        return await update_transaction_payload_workflow(
            self,
            UUID(transaction_id),
            new_payload,
            started_at=started_at
        )

    return async_to_sync(critical_section)()


@shared_task(bind=True, acks_late=True, max_retries=100)
def prepare_send_stars_task(
        self: Task[P,R],
        transaction_id: str,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float | None
) -> str:
    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        return await prepare_send_stars_workflow(
            self,
            UUID(transaction_id),
            parsed_payload,
            payment_method,
            started_at=started_at
        )

    lock = sync_get_lock_or_retry(self, get_lock_payment_transaction(transaction_id))

    try:
        return async_to_sync(critical_section)()
    finally:
        lock.release()


@shared_task(bind=True, max_retries=100)
def send_stars_task(
        self: Task[P,R],
        transaction_id: str,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float | None
) -> str:
    async def critical_section() -> str:
        nonlocal started_at
        if started_at is None:
            started_at = time.time()

        return await send_stars_workflow(
            self,
            UUID(transaction_id),
            parsed_payload,
            payment_method,
            started_at=started_at
        )

    lock = sync_get_lock_or_retry(self, get_lock_payment_transaction(transaction_id))

    try:
        return async_to_sync(critical_section)()
    finally:
        lock.release()


async def prepare_send_stars_workflow(
        celery_task: Task[P,R],
        transaction_id: UUID,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float
) -> str:
    kwargs = cast(dict[str, object], celery_task.request.kwargs or {}).copy()  # noqa
    kwargs["started_at"] = started_at

    timeout = 300.0  # 5 минут

    transaction = await safe_get_transaction_with_retries(
        celery_task, started_at, kwargs, timeout,
        transaction_id
    )
    if transaction is None:
        return f"transaction {transaction_id} is not found for preparing stars send"

    new_status = TransactionStatus.SENDING
    if not is_change_status_allowed(transaction.status, new_status):
        _ = update_transaction_payload_task.apply_async(
            args=(str(transaction_id), {"requested_status": new_status}),
            kwargs={"started_at": None}
        )
        return f"change status from {transaction.status} to {new_status} for {transaction_id} is not allowed"

    is_changed_successfully = await safe_set_status_for_transaction_id_with_retries(
        celery_task, started_at, kwargs, timeout,
        transaction_id, new_status
    )
    if not is_changed_successfully:  # Точка невозврата - если статус уже был "В ДОСТАВКЕ", ничего не делаем
        return f"transaction {transaction_id} was already {new_status}"

    _ = send_stars_task.apply_async(
        args=(str(transaction_id), parsed_payload, payment_method),
        kwargs={"started_at": None}
    )
    return f"sent send_stars_task for transaction {transaction_id}"


async def send_stars_workflow(
        celery_task: Task[P,R],
        transaction_id: UUID,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float
) -> str:
    kwargs = cast(dict[str, object], celery_task.request.kwargs or {}).copy()  # noqa
    kwargs["started_at"] = started_at

    timeout = 600.0  # 10 минут

    transaction = await safe_get_transaction_with_retries(
        celery_task, started_at, kwargs, timeout,
        transaction_id
    )
    if transaction is None:
        return f"transaction {transaction_id} is not found for sending stars"

    if transaction.status != TransactionStatus.SENDING:
        return f"transaction {transaction_id} doesn't have status SENDING"

    def create_task_save_error_to_db(msg_to_save: str) -> None:
        _ = update_transaction_payload_task.apply_async(
            args=(str(transaction_id), {"err_msg": str(msg_to_save)}),
            kwargs={"started_at": None}
        )

    try:
        response, err_msg = await create_fragment_transaction_if_not_sent_with_retries(
            celery_task, started_at, kwargs, timeout,
            transaction
        )

    except Exception as exc:
        create_task_save_error_to_db(str(exc))
        _ = sync_save_status_by_key(
            ServicesNames.FRAGMENT, transaction_id,
            TransactionStatus.FAILED
        )
        _ = update_transaction_status_task.apply_async(
            args=(str(transaction_id), parsed_payload, payment_method),
            kwargs={"started_at": None}
        )
        return f"exception occurred trying to send stars for transaction {transaction_id}"

    new_status = response["status"]
    fragment_tx_id = response.get("id", None)

    if fragment_tx_id is not None:
        _ = sync_save_status_by_key(ServicesNames.FRAGMENT__FROM_CREATION, transaction_id, new_status)
        _ = update_fragment_tx_task.apply_async(
            args=(str(fragment_tx_id), str(transaction_id)),
            kwargs={"started_at": None}
        )

    _ = sync_save_status_by_key(
        ServicesNames.FRAGMENT, transaction_id,
        transform_into_internal_status_or_keep_original(new_status, ServicesNames.FRAGMENT),
        if_not_exists=True
    )
    _ = update_transaction_status_task.apply_async(
        args=(str(transaction_id), parsed_payload, payment_method),
        kwargs={"started_at": None}
    )

    if err_msg:
        create_task_save_error_to_db(err_msg)
        return err_msg

    result_msg = f"transaction {transaction_id} fragment status: {new_status}"

    if fragment_tx_id is None:
        result_msg += "; fragment_tx_id is None"

    return result_msg


async def update_transaction_payload_workflow(
        celery_task: Task[P,R],
        transaction_id: UUID,
        new_payload: dict[str, object],
        *,
        started_at: float
) -> str:
    kwargs = cast(dict[str, object], celery_task.request.kwargs or {}).copy()  # noqa
    kwargs["started_at"] = started_at

    timeout = 300.0  # 5 минут

    transaction = await safe_get_transaction_with_retries(
        celery_task, started_at, kwargs,
        timeout,
        transaction_id
    )
    if transaction is None:
        return f"transaction {transaction_id} is not found for payload update"

    result = await safe_update_transaction_payload_with_retries(
        celery_task, started_at, kwargs, timeout,
        transaction, new_payload
    )

    if result is None:
        return f"updating payload for transaction {transaction_id} is timed out"

    if not result[0]:
        return f"failed to update payload for transaction {transaction_id}"

    return f"updated payload for transaction {transaction_id}"


async def update_transaction_status_workflow(
        celery_task: Task[P,R],
        transaction_id: UUID,
        new_status: str,
        parsed_payload: PaymentPayloadDict | None,
        payment_method: str,
        *,
        started_at: float
) -> tuple[bool, str]:
    kwargs = cast(dict[str, object], celery_task.request.kwargs or {}).copy()  # noqa
    kwargs["started_at"] = started_at

    timeout = 600.0  # 10 минут

    new_status = cast(TransactionStatus, new_status)

    transaction = await safe_get_transaction_with_retries(
        celery_task, started_at, kwargs,
        timeout,
        transaction_id
    )
    if transaction is None:
        if parsed_payload is None:
            return False, f"transaction {transaction_id} is not found but parsed_payload is None"

        transaction = await safe_create_transaction_with_retries(
            celery_task, started_at, kwargs, timeout,
            transaction_id, new_status, parsed_payload, payment_method
        )
        if transaction is None:
            return False, f"transaction {transaction_id} is not found but couldn't recreate transaction"

    if not is_change_status_allowed(transaction.status, new_status):
        _ = update_transaction_payload_task.apply_async(
            args=(str(transaction_id), {"requested_status": new_status}),
            kwargs={"started_at": None}
        )
        return True, f"change from {transaction.status} to {new_status} for transaction {transaction_id} is not allowed"

    _, transaction = await safe_set_status_for_transaction_obj_with_retries(
        celery_task, started_at, kwargs, timeout,
        transaction, new_status
    )

    parse_mode = ParseMode.HTML.value

    promo_discount = transaction.metadata_info.promo_discount
    if promo_discount is not None:
        promo_discount = str(promo_discount)

    _ = update_order_message_task.apply_async(
        args=(
            parse_mode,
            transaction.telegram_user.telegram_id,
            transaction.message_id,
            str(transaction_id),
            transaction.amount_stars,
            f"{transaction.amount_fiat:.2f}",
            transaction.target_username,
            transaction.pay_url,
            transaction.metadata_info.promo_name,
            promo_discount
        ),
        kwargs={"started_at": None}
    )

    if new_status == TransactionStatus.PROCESSING:
        _ = prepare_send_stars_task.apply_async(
            args=(str(transaction_id), parsed_payload, payment_method),
            kwargs={"started_at": None}
        )

    elif new_status == TransactionStatus.CANCELLED:
        cleanup_logger.info(f"Транзакция {transaction_id} помечена CANCELLED на удаление")

    return True, f"set status {new_status} for transaction {transaction_id}"
