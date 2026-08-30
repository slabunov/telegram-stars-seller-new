"""
Разовая сверка зависших заказов Fragment.

Периодическая задача `poll_unfinished_fragment_orders_task` смотрит только на свежие
транзакции (`FRAGMENT_POLL_MAX_AGE_HOURS`). Эта команда - для тех заказов, которые
зависли раньше, чем появился опрос::

    python manage.py sync_fragment_orders --dry-run
    python manage.py sync_fragment_orders
    python manage.py sync_fragment_orders --transaction 305344b6-287f-4518-87d5-6b36eeb2da40

Команда только ставит задачи в очередь Celery, поэтому воркер должен быть запущен.
"""

from __future__ import annotations

from uuid import UUID
from typing import final, override

from asgiref.sync import async_to_sync

from django.core.management.base import BaseCommand, CommandParser

from core.domain.enums import get_translation
from core.models import FragmentTransaction, Transaction
from core.tasks.periodic.fragment_orders import WAITING_STATUSES, push_fragment_status


@final
class Command(BaseCommand):
    help = "Перечитывает статусы заказов fragment-api для транзакций, застрявших до финального статуса"

    @override
    def add_arguments(self, parser: CommandParser) -> None:
        _ = parser.add_argument(
            "--transaction",
            action="append",
            default=[],
            metavar="UUID",
            help="ID конкретной транзакции; можно указать несколько раз. По умолчанию - все зависшие"
        )
        _ = parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Только показать, что вернул Fragment, ничего не менять"
        )

    @override
    def handle(self, *args: object, **options: object) -> None:
        transaction_ids = [str(tx_id) for tx_id in (options.get("transaction") or [])]  # pyright: ignore[reportAny]
        dry_run = bool(options.get("dry_run"))

        async_to_sync(self._sync)(transaction_ids, dry_run)

    async def _sync(self, transaction_ids: list[str], dry_run: bool) -> None:
        from core.integrations.fragment.client import FragmentClient
        from core.integrations.webhook_utils import ServicesNames, transform_into_internal_status_or_keep_original
        from core.ioc import close_container, get_container

        query = Transaction.objects.all()
        if transaction_ids:
            query = query.filter(id__in=[UUID(tx_id) for tx_id in transaction_ids])
        else:
            query = query.filter(status__in=WAITING_STATUSES)

        waiting = [t async for t in query.order_by("created_at")]
        if not waiting:
            self.stdout.write("Зависших заказов не найдено")
            return

        fragment_txs: dict[UUID, FragmentTransaction] = {
            row.id_from_payment_api: row
            async for row in (
                FragmentTransaction.objects
                .filter(id_from_payment_api__in=[t.id for t in waiting])
                .order_by("created_at")
            )
        }

        client = await get_container().get(FragmentClient)
        advanced = 0

        try:
            for txn in waiting:
                fragment_tx = fragment_txs.get(txn.id)
                if fragment_tx is None:
                    self.stdout.write(self.style.WARNING(
                        f"{txn.id} [{get_translation(txn.status)}]: заказа во Fragment нет - звёзды не отправлялись"
                    ))
                    continue

                try:
                    order = await client.get_order(fragment_tx.fragment_id, timeout=20.0, connect=10.0)
                except Exception as exc:
                    self.stdout.write(self.style.ERROR(
                        f"{txn.id}: {exc.__class__.__name__}: {exc}"
                    ))
                    continue

                if order is None:
                    self.stdout.write(self.style.WARNING(
                        f"{txn.id}: заказ {fragment_tx.fragment_id} не найден во Fragment"
                    ))
                    continue

                raw_status = str(order.get("status", ""))
                internal_status = str(
                    transform_into_internal_status_or_keep_original(raw_status, ServicesNames.FRAGMENT)
                )

                line = (
                    f"{txn.id} [{get_translation(txn.status)}] "
                    f"-> fragment {raw_status} -> {get_translation(internal_status)}"
                )

                if internal_status == txn.status:
                    self.stdout.write(f"{line} (без изменений)")
                    continue

                if dry_run:
                    self.stdout.write(self.style.NOTICE(f"{line} (dry-run)"))
                    continue

                push_fragment_status(txn.id, fragment_tx.fragment_id, raw_status, internal_status)
                advanced += 1
                self.stdout.write(self.style.SUCCESS(line))

        finally:
            await close_container()

        if dry_run:
            self.stdout.write(f"\nПроверено {len(waiting)}, изменений не вносилось (--dry-run)")
        else:
            self.stdout.write(f"\nПроверено {len(waiting)}, отправлено в конвейер {advanced}")

        if advanced:
            self.stdout.write("Финальные статусы проставит celery-воркер - убедись, что он запущен")
