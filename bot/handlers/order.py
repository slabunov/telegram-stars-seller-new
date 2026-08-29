import re
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from math import ceil
from typing import Literal, cast, overload

from dishka import FromDishka

from httpx import ConnectTimeout, ReadTimeout

from telegram import Update, Message
from telegram.ext import ContextTypes, ConversationHandler

from django.conf import settings

from bot.handlers.start import running_users

from bot.keyboards.error import KeyboardMethodError

from bot.renderers.base import delete_message
from bot.renderers.main import send_empty_username_alert
from bot.renderers.order import (
    send_small_order_error,
    show_choose_recipient,
    show_custom_quantity_input,
    edit_order_creating_failed_message,
    show_pay_url_not_provided,
    show_payment_methods,
    show_large_order_warning,
    show_enter_username,
    show_searching_username,
    show_user_not_found,
    show_order_confirmation, edit_order_created_message, edit_order_creating_message
)

from bot.notifications.admin import notify_admin_about_order_creation
from bot.utils.active_conversation import ensure_use_active_conversation_with_callback
from bot.utils.channel_subscription import require_subscription
from bot.callbacks import PaymentMethodCallback, RecipientModeCallback, FixedQuantityCallback, manage_callback_data
from bot.context import get_view_context
from bot.enums import BackDestination, RecipientMode
from bot.states import BotConversationState

from core.integrations.fragment.client import FragmentClient
from core.integrations.utils import retries_with_tenacity
from core.repositories.utils import db_action_with_tenacity, db_action_or_exception_with_tenacity
from core.services.payment import PaymentService
from core.services.promo_code import PromoCodeService
from core.services.support import SupportService
from core.services.transaction import TransactionService
from core.services.user import UserService
from core.ioc import inject


logger = logging.getLogger(__name__)


@ensure_use_active_conversation_with_callback
@require_subscription(BackDestination.CHOOSE_QUANTITY)
async def handle_fixed_quantity(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.CHOOSE_RECIPIENT] | int:
    async with manage_callback_data(update, FixedQuantityCallback) as cb_data:
        if isinstance(cb_data, int):
            assert cb_data == ConversationHandler.END
            return cb_data

        ctx = get_view_context(context)
        ctx.order.quantity = cb_data.amount

        _ = await show_choose_recipient(update, context)
        return BotConversationState.CHOOSE_RECIPIENT


@ensure_use_active_conversation_with_callback
@require_subscription(BackDestination.CHOOSE_QUANTITY)
async def handle_custom_quantity_btn(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.CUSTOM_QUANTITY_INPUT]:
    _ = await show_custom_quantity_input(update, context)
    return BotConversationState.CUSTOM_QUANTITY_INPUT


@overload
async def _handle_custom_quantity_input_helper(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[
    BotConversationState.CUSTOM_QUANTITY_INPUT,
    BotConversationState.LARGE_ORDER_WARNING,
    BotConversationState.CHOOSE_RECIPIENT
]: ...


@inject
async def _handle_custom_quantity_input_helper(
        update: Update, context: ContextTypes.DEFAULT_TYPE,
        *,
        support_service: FromDishka[SupportService]  # noqa
) -> Literal[
    BotConversationState.CUSTOM_QUANTITY_INPUT,
    BotConversationState.LARGE_ORDER_WARNING,
    BotConversationState.CHOOSE_RECIPIENT
]:
    user_id = update.effective_user.id
    if user_id in running_users:
        return BotConversationState.CUSTOM_QUANTITY_INPUT

    running_users.add(user_id)

    try:
        user_msg = cast(Message, update.message)  # noqa

        text = user_msg.text
        if text is None or not text.isdigit():
            return BotConversationState.CUSTOM_QUANTITY_INPUT

        amount = int(text)

        if amount < 50:
            _ = await send_small_order_error(update)
            return BotConversationState.CUSTOM_QUANTITY_INPUT

        if amount > 10000:  # Условный лимит
            url = await support_service.get_support_url()
            _ = await show_large_order_warning(update, context, amount, url)
            return BotConversationState.LARGE_ORDER_WARNING

        ctx = get_view_context(context)
        ctx.order.quantity = amount

        _ = await show_choose_recipient(update, context)
        return BotConversationState.CHOOSE_RECIPIENT

    finally:
        running_users.discard(user_id)


# Срабатывает на ввод пользователя, поэтому @ensure_use_active_conversation_with_callback не нужен
@require_subscription(BackDestination.CUSTOM_QUANTITY_INPUT)
async def handle_custom_quantity_input(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[
    BotConversationState.CUSTOM_QUANTITY_INPUT,
    BotConversationState.LARGE_ORDER_WARNING,
    BotConversationState.CHOOSE_RECIPIENT
]:
    return await _handle_custom_quantity_input_helper(update, context)


@overload
async def _handle_recipient_mode_helper(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.CHOOSE_PAYMENT, BotConversationState.ENTER_GIFT_USERNAME] | int: ...


@inject
async def _handle_recipient_mode_helper(
        update: Update, context: ContextTypes.DEFAULT_TYPE,
        *,
        promo_service: FromDishka[PromoCodeService]  # noqa
) -> Literal[BotConversationState.CHOOSE_PAYMENT, BotConversationState.ENTER_GIFT_USERNAME] | int:
    async with manage_callback_data(update, RecipientModeCallback) as cb_data:
        if isinstance(cb_data, int):
            assert cb_data == ConversationHandler.END
            return cb_data

        ctx = get_view_context(context)
        ctx.order.recipient_mode = cb_data.mode

        if cb_data.mode == RecipientMode.SELF:
            # Нужно указывать пустым, так как сюда можно вернуться с предыдущих шагов, где он мог быть заполнен
            ctx.order.target_username = ""
            stars_count = cast(int, ctx.order.quantity)  # noqa

            active_promo = await db_action_with_tenacity(
                promo_service.get_active_promo_for_telegram_user_id, update.effective_user.id
            )

            _ = await show_payment_methods(
                update, context,
                stars_count,
                active_promo,
                ctx.order.target_username
            )
            return BotConversationState.CHOOSE_PAYMENT

        else:
            _ = await show_enter_username(update, context)
            return BotConversationState.ENTER_GIFT_USERNAME


@ensure_use_active_conversation_with_callback
@require_subscription(BackDestination.CHOOSE_RECIPIENT)
async def handle_recipient_mode(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.CHOOSE_PAYMENT, BotConversationState.ENTER_GIFT_USERNAME] | int:
    return await _handle_recipient_mode_helper(update, context)


@overload
async def _handle_gift_username_helper(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ENTER_GIFT_USERNAME, BotConversationState.CHOOSE_PAYMENT]: ...


@inject
async def _handle_gift_username_helper(
        update: Update, context: ContextTypes.DEFAULT_TYPE,
        *,
        fragment_client: FromDishka[FragmentClient],  # noqa
        promo_service: FromDishka[PromoCodeService]  # noqa
) -> Literal[BotConversationState.ENTER_GIFT_USERNAME, BotConversationState.CHOOSE_PAYMENT]:
    user_id = update.effective_user.id
    if user_id in running_users:
        return BotConversationState.ENTER_GIFT_USERNAME

    running_users.add(user_id)

    try:
        user_msg = update.message

        username = cast(str, user_msg.text)  # noqa
        username_pattern = re.compile(r"^@?[a-zA-Z][a-zA-Z0-9_]{2,31}$")
        if not username_pattern.search(username):
            return BotConversationState.ENTER_GIFT_USERNAME

        ctx = get_view_context(context)

        _ = await delete_message(ctx.active_conversation)
        msg_searching = await show_searching_username(update, context, username)

        is_found = await retries_with_tenacity(
            fragment_client.resolve_username, username, timeout=30.0, connect=10.0
        )

        _ = await delete_message(msg_searching)

        if not is_found:
            _ = await show_user_not_found(update, context, username)
            return BotConversationState.ENTER_GIFT_USERNAME

        ctx.order.target_username = username.lstrip("@")

        stars_count = cast(int, ctx.order.quantity)  # noqa

        active_promo = await db_action_with_tenacity(
            promo_service.get_active_promo_for_telegram_user_id, user_id
        )

        _ = await show_payment_methods(update, context, stars_count, active_promo, username)

        return BotConversationState.CHOOSE_PAYMENT

    finally:
        running_users.discard(user_id)


# Срабатывает на ввод пользователя, поэтому @ensure_use_active_conversation_with_callback не нужен
@require_subscription(BackDestination.ENTER_GIFT_USERNAME)
async def handle_gift_username(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ENTER_GIFT_USERNAME, BotConversationState.CHOOSE_PAYMENT]:
    return await _handle_gift_username_helper(update, context)


@overload
async def _handle_payment_method_helper(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ORDER_CONFIRMATION] | int: ...


@inject
async def _handle_payment_method_helper(
        update: Update, context: ContextTypes.DEFAULT_TYPE,
        *,
        promo_service: FromDishka[PromoCodeService]  # noqa
) -> Literal[BotConversationState.ORDER_CONFIRMATION] | int:
    async with manage_callback_data(update, PaymentMethodCallback) as cb_data:
        if isinstance(cb_data, int):
            assert cb_data == ConversationHandler.END
            return cb_data

        ctx = get_view_context(context)

        stars = ctx.order.quantity
        if stars is None:
            raise AttributeError("order stars amount is None")
        price = cb_data.price

        ctx.order.price = str(price)
        ctx.order.payment_method = cb_data.method
        ctx.order.payment_external_id = cb_data.method_external_id
        ctx.order.payment_api = cb_data.method_api

        active_promo = await db_action_with_tenacity(
            promo_service.get_active_promo_for_telegram_user_id, update.effective_user.id
        )
        if active_promo is None:
            ctx.order.is_used_promo_code = False

        _ = await show_order_confirmation(
            update, context,
            stars, price, ctx.order.target_username, active_promo
        )
        return BotConversationState.ORDER_CONFIRMATION


@ensure_use_active_conversation_with_callback
@require_subscription(BackDestination.CHOOSE_PAYMENT)
async def handle_payment_method(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ORDER_CONFIRMATION] | int:
    return await _handle_payment_method_helper(update, context)


@overload
async def _handle_order_confirmed_helper(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ORDER_CONFIRMATION, BotConversationState.ORDER_CONFIRMED]: ...


@inject
async def _handle_order_confirmed_helper(
        update: Update, context: ContextTypes.DEFAULT_TYPE,
        *,
        fragment_client: FromDishka[FragmentClient], payment_service: FromDishka[PaymentService],  # noqa
        transaction_service: FromDishka[TransactionService], promo_service: FromDishka[PromoCodeService],  # noqa
        user_service: FromDishka[UserService], support_service: FromDishka[SupportService]  # noqa
) -> Literal[BotConversationState.ORDER_CONFIRMATION, BotConversationState.ORDER_CONFIRMED]:
    ctx = get_view_context(context)

    if ctx.order.recipient_mode == RecipientMode.SELF and update.effective_user.username is None:
        _ = await send_empty_username_alert(update)
        return BotConversationState.ORDER_CONFIRMATION

    # Если maintenance_mode, выбросится исключение для обработки в error_handler
    await db_action_with_tenacity(payment_service.ensure_no_maintenance_mode)

    amount_stars = ctx.order.quantity
    price = ctx.order.price
    external_method_id = ctx.order.payment_external_id
    method_api = ctx.order.payment_api

    if amount_stars is None:
        raise AttributeError("amount_stars is None во время создания заказа")
    if price is None:
        raise AttributeError("price is None во время создания заказа")
    if external_method_id is None:
        raise AttributeError("method_id is None во время создания заказа")
    if method_api is None:
        raise AttributeError("method_api is None во время создания заказа")

    # TODO: сделать механизм удержания баланса
    # Если не получится определить, хватает ли средств для перевода звёзд, выбросится исключение для обработки в error_handler
    await retries_with_tenacity(
        fragment_client.check_is_enough_currency_for_stars, amount_stars, timeout=30.0, connect=10.0
    )

    try:
        price = Decimal(price)
    except ValueError:
        raise KeyboardMethodError("Цена должна быть в формате Decimal")

    # Platega ожидает числовой ID метода, PayPear - строковый type (`sbp`, `card`)
    if "platega" in method_api.lower():
        try:
            external_method_id = int(external_method_id)
        except ValueError:
            raise KeyboardMethodError("Внешний ID метода оплаты должен быть целым числом для используемого API")
    else:
        external_method_id = str(external_method_id)

    active_promo = await db_action_with_tenacity(
        promo_service.get_active_promo_for_telegram_user_id, update.effective_user.id
    )
    if active_promo is None and ctx.order.is_used_promo_code:
        _ = await update.callback_query.answer(
            text="Был активирован промокод, но произошла автоматическая деактивация. Активируй промокод снова",
            show_alert=True
        )
        return BotConversationState.ORDER_CONFIRMATION

    order_msg = update.effective_message
    if order_msg is None:
        raise RuntimeError("По какой-то причине сообщение заказа отсутствует при создании заказа")

    try:
        payment_dto, parsed_payload = await db_action_with_tenacity(payment_service.create_payment,
            user_id=update.effective_user.id,
            message_id=order_msg.message_id,
            price=price,
            stars_count=amount_stars,
            payment_api=method_api,
            method=external_method_id,
            target_username=ctx.order.target_username,
            promo=active_promo
        )

    except (ConnectTimeout, ReadTimeout):
        _ = await update.callback_query.answer(
            text="В данный момент у платёжной системы наблюдаются сетевые проблемы. Попробуй снова через 5 минут",
            show_alert=True
        )
        return BotConversationState.ORDER_CONFIRMATION

    pay_url = payment_dto.pay_url
    if pay_url is None:
        _ = await show_pay_url_not_provided(update, context, await support_service.get_support_url())
        return BotConversationState.ORDER_CONFIRMATION

    parsed_payload["pay_url"] = pay_url

    _ = await db_action_with_tenacity(transaction_service.create_transaction,
        payment_dto.transaction_id,
        parsed_payload,
        payment_method=f"{method_api} - {external_method_id}",
        expires_in=payment_dto.expires_in
    )

    ctx.order.checkout_transaction_id = str(payment_dto.transaction_id)
    ctx.order.checkout_url = payment_dto.pay_url

    actual_expires_in = datetime.strptime(payment_dto.expires_in, "%H:%M:%S")
    expires_in_td = timedelta(hours=actual_expires_in.hour, minutes=actual_expires_in.minute,
                              seconds=actual_expires_in.second)
    expires_in_minutes = str(ceil(expires_in_td.total_seconds() / 60))

    msg = await edit_order_creating_message(order_msg)
    if msg is None:
        raise RuntimeError(f"Не получилось изменить сообщение с id {order_msg.message_id}")

    db_action = await db_action_or_exception_with_tenacity(
        transaction_service.save_message_id, payment_dto.transaction_id, msg.message_id
    )
    if isinstance(db_action, Exception):
        is_changed_successfully = False
    else:
        is_changed_successfully, _ = db_action

    if is_changed_successfully:
        msg = await edit_order_created_message(
            msg,
            amount_stars, payment_dto.price, pay_url, payment_dto.transaction_id,
            expires_in_minutes, ctx.order.target_username, active_promo
        )
        if msg is None:
            raise RuntimeError(f"Не получилось изменить сообщение с id {order_msg.message_id}")

        _ = await db_action_with_tenacity(
            user_service.update_active_promo, update.effective_user.id, None
        )

        buyer_username = update.effective_user.username
        if buyer_username is None:
            buyer_username = str(update.effective_user.id)

        if settings.NOTIFY_ABOUT_ORDERS:  # pyright: ignore[reportAny]
            _ = context.application.create_task(notify_admin_about_order_creation(  # pyright: ignore[reportUnknownMemberType]
                context.bot,
                amount_stars, payment_dto.price,
                method_api, external_method_id,
                buyer_username, ctx.order.target_username,
                active_promo,
                payment_dto.transaction_id
            ))

        return BotConversationState.ORDER_CONFIRMED

    else:
        msg = await edit_order_creating_failed_message(msg)
        err_msg = (
            f"При попытке сохранить id сообщения заказа или транзакция {payment_dto.transaction_id} не была найдена, "
            f"или произошла непредвиденная ошибка"
        )
        if msg is None:
            err_msg += f". Также не получилось обновить сообщения заказа с id {order_msg.message_id}"
        logger.exception(err_msg)
        return BotConversationState.ORDER_CONFIRMATION


@ensure_use_active_conversation_with_callback
@require_subscription(BackDestination.ORDER_CONFIRMATION)
async def handle_order_confirmed(
        update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Literal[BotConversationState.ORDER_CONFIRMATION, BotConversationState.ORDER_CONFIRMED]:
    return await _handle_order_confirmed_helper(update, context)
