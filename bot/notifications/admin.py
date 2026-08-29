from decimal import Decimal
from uuid import UUID
from typing import overload

from dishka import FromDishka

from django.conf import settings

from telegram import Bot, Message
from telegram.constants import ParseMode

from tenacity import retry

from bot.renderers.order import get_promo_and_price_sentences

from core.domain.tenacity_utils import TelegramRetryConfig
from core.repositories.utils import db_action_or_exception_with_tenacity
from core.services.payment import PaymentService
from core.ioc import inject
from core.models import PaymentMethod, PromoCode


_retry_config = TelegramRetryConfig().asdict


@retry(**_retry_config)
async def _notify(bot: Bot, message_thread_id: int | None, text: str) -> Message:
    return await bot.send_message(
        chat_id=settings.ADMIN_CHAT_ID, message_thread_id=message_thread_id,  # pyright: ignore[reportAny]
        text=text, parse_mode=ParseMode.HTML
    )


@overload
async def notify_admin_about_order_creation(  # noqa  # pyright: ignore[reportInconsistentOverload]
        bot: Bot,
        amount_stars: int, price: Decimal, method_api: str, external_method_id: int | str,
        buyer_username: str, target_username: str,
        active_promo: PromoCode | None,
        transaction_id: UUID | str
) -> Message: ...

@inject
async def notify_admin_about_order_creation(
        bot: Bot,
        amount_stars: int, price: Decimal, method_api: str, external_method_id: int | str,
        buyer_username: str, target_username: str,
        active_promo: PromoCode | None,
        transaction_id: UUID | str,
        *,
        payment_service: FromDishka[PaymentService]
) -> Message:
    promo_name = ""
    promo_discount = None
    promo_id = None
    if active_promo:
        promo_name = active_promo.name
        promo_discount = active_promo.discount
        promo_id = active_promo.id
    promo_sentence, price_sentence = get_promo_and_price_sentences(price, promo_name, promo_discount)
    if promo_id is not None:
        promo_sentence += f"ID промокода — <code>{active_promo.id}</code>\n"

    payment_method = await db_action_or_exception_with_tenacity(
        payment_service.get_payment_method, method_api, external_method_id
    )

    payment_name = "не получилось определить"
    payment_commission_percent = "не получилось определить"
    if isinstance(payment_method, PaymentMethod):
        payment_name = payment_method.name
        payment_commission_percent = f"{payment_method.commission_percent:.2f}%"

    if not buyer_username.isdecimal():
        buyer_username = f"@{buyer_username.lstrip("@")}"
    if target_username:
        target_username = f"@{target_username.lstrip("@")}"

    text = (
        f"📦 <b>Заказ ждёт оплату!</b>\n\n"
        f"{promo_sentence}"
        f"Пополним — ⭐ {amount_stars} звёзд\n"
        f"{price_sentence}"
        f"Метод оплаты — {payment_name}\n"
        f"Комиссия метода оплаты — {payment_commission_percent}\n"
        f"Покупатель — {buyer_username}\n"
        f"Кому покупает — {target_username if target_username else 'Себе'}\n"
        f"🆔 ID заказа: <code>{transaction_id}</code>\n\n"
    )

    return await _notify(bot, settings.ADMIN_ORDERS_TOPIC_ID, text)  # pyright: ignore[reportAny]
