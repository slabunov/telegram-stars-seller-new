import os
import asyncio
import logging

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_process_shutdown, after_setup_logger, after_setup_task_logger


logger = logging.getLogger(__name__)

_ = os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


class _SilenceHeartbeatTasks(logging.Filter):
    _NEEDLES = (
        "poll_pending_paypear_payments_task", "poll-pending-paypear-payments",
        "poll_unfinished_fragment_orders_task", "poll-unfinished-fragment-orders",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.INFO:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        return not any(needle in message for needle in self._NEEDLES)


_heartbeat_filter = _SilenceHeartbeatTasks()

for _logger_name in ("celery.app.trace", "celery.worker.strategy", "celery.beat", "celery"):
    logging.getLogger(_logger_name).addFilter(_heartbeat_filter)


@after_setup_logger.connect
@after_setup_task_logger.connect
def _reapply_heartbeat_filter(logger: logging.Logger, **_: object) -> None:  # noqa
    logger.addFilter(_heartbeat_filter)


app = Celery("config")


celery_settings = app.config_from_object("django.conf:settings", namespace="CELERY")

app.conf.beat_schedule = {
    "daily-check-clean-two-week-cancelled-transactions": {
        "task": "core.tasks.periodic.transactions.cleanup_two_week_cancelled_transactions_task",
        "schedule": crontab(hour=2, minute=0),  # Каждый день в 02:00
    },
    "daily-check-deactivate-unused-promo-codes": {
        "task": "core.tasks.periodic.promo_codes.deactivate_unused_promo_codes_task",
        "schedule": crontab(hour=2, minute=30),  # Каждый день в 02:30
    },
    "poll-pending-paypear-payments": {
        "task": "core.tasks.periodic.payments.poll_pending_paypear_payments_task",
        "schedule": float(os.environ.get("PAYPEAR_POLL_SECONDS", "5")),  # опрос статусов PayPear
    },
    "poll-unfinished-fragment-orders": {
        "task": "core.tasks.periodic.fragment_orders.poll_unfinished_fragment_orders_task",
        "schedule": float(os.environ.get("FRAGMENT_POLL_SECONDS", "15")),  # опрос статусов заказов Fragment
    },
}


app.autodiscover_tasks()


# Этот сигнал срабатывает при выключении каждого отдельного процесса-воркера
@worker_process_shutdown.connect
def shutdown_worker(**_: object):
    logger.info("Celery worker shutting down...")
    from general.resource_management import close_resources, Where
    asyncio.run(close_resources(Where("in Celery")))
