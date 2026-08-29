import logging
import traceback
from pickle import UnpicklingError
from typing import overload

from httpx import ConnectError

from dishka import FromDishka

from telegram import Update
from telegram.error import NetworkError
from telegram.ext import ContextTypes, InvalidCallbackData

from general.utils import json_dumps

from bot.keyboards.error import KeyboardMethodError
from bot.keyboards.support import build_support_kb
from bot.renderers.base import delete_message, send_new_message
from bot.context import get_view_context

from core.integrations.fragment.errors import (
    FragmentAPIError,
    FragmentAPINotEnoughBalanceError,
    FragmentAPITemporaryError,
    FragmentAPITooManyRequests
)
from core.integrations.paypear.errors import PayPearAPIError
from core.integrations.platega.errors import PlategaAPIError
from core.services.payment import MaintenanceModeException, NoUsernameError
from core.services.redis_service import DecodingRedisDataError
from core.services.support import SupportService
from core.ioc import inject


logger = logging.getLogger(__name__)


@overload
async def error_handler(  # noqa  # pyright: ignore[reportInconsistentOverload]
        update: object | None, context: ContextTypes.DEFAULT_TYPE
) -> None: ...


@inject
async def error_handler(
        update: object | None, context: ContextTypes.DEFAULT_TYPE,
        *,
        support_service: FromDishka[SupportService]  # noqa
) -> None:
    error = context.error
    error_type = error.__class__.__name__
    error_msg = str(error)

    log_msg = "Произошло исключение при обработке обновления:"
    if not (isinstance(error, NetworkError) and "ReadError" in error_msg):
        tb_list = traceback.format_exception(None, error, error.__traceback__)
        tb_string = "".join(tb_list)

        update_str = update.to_dict() if isinstance(update, Update) else str(update)

        log_msg += (
            f"\nUpdate: {json_dumps(update_str, indent=2)}\n"
            f"Traceback: {tb_string}"
        )
        logger.error(log_msg)

    else:
        logger.error(f" {error_type}: {error_msg}")

    if not isinstance(update, Update):
        return

    support_url = await support_service.get_support_url()
    reply_markup = await build_support_kb(support_url)

    if isinstance(error, (FragmentAPIError, PlategaAPIError, PayPearAPIError)):
        text = (
            "❌ <b>Произошла ошибка!</b>\n\n"
            "Попробуй последнее действие снова или вернись назад, если есть возможность. Либо начинай новый заказ "
            "с помощью /start или обратись в тех. поддержку с текстом ошибки\n\n"
            f"Текст ошибки:\n<pre>{error_type}: {error_msg}</pre>"
        )

    elif isinstance(error, FragmentAPITooManyRequests):
        retry_after = str(error.retry_after) if error.retry_after else ""
        text = (
            f"⚠️ <b>Fragment перегружен...</b>\n\n"
            f"{
            'Попробуй последнее действие снова через ' + retry_after + ' секунд или обратись в тех. поддержку' if retry_after
            else 'Обратись в тех. поддержку'
            }"
            f" с текстом ошибки\n\n"
            f"Текст ошибки:\n<pre>{error_type}: {error_msg}</pre>"
        )

    elif isinstance(error, FragmentAPITemporaryError):
        text = f"⚠️ <b>Временные неполадки...</b>\n\n{error.bot_message}"

    elif isinstance(error, FragmentAPINotEnoughBalanceError):
        text = (
            f"💰 <b>На балансе бота не хватает средств для перевода звёзд :(</b>\n\n"
            f"Выбери меньшее количество звёзд, или попробуй заново через 5 минут, или, "
            f"если ошибка останется, обратись в тех. поддержку"
        )

    elif isinstance(error, NoUsernameError):
        text = (
            f"⚠️ <b>Не получилось определить username...</b>\n\n"
            f"Для перевода звёзд он обязателен, поэтому попробуй сделать заказ заново"
        )

    elif isinstance(error, KeyboardMethodError):
        text = (
            "❌ <b>Произошла ошибка!</b>\n\n"
            "Метод оплаты недоступен по техническим причинам. Попробуй другой метод оплаты или вернись назад. Либо "
            "обратись в тех. поддержку с текстом ошибки\n\n"
            f"Текст ошибки:\n<pre>{error_type}: {error_msg}</pre>"
        )

    elif isinstance(error, MaintenanceModeException):
        text = (
            "⚠️ <b>Извини, бот на техническом перерыве...</b>\n\n"
            "Если оформлялся заказ, то он был отменён, поэтому в таком случае нужно начать новый с помощью /start"
        )

        ctx = get_view_context(context)
        try:
            _ = await delete_message(ctx.active_conversation)
        except Exception:  # noqa
            pass
        ctx.active_conversation = None

    elif isinstance(error, InvalidCallbackData) and update.callback_query:
        text = (
            "❌ Не получилось обработать кнопку...\n"
            "Начни заказ снова с помощью /start или обратись в тех. поддержку, если ошибка останется"
        )
        _ = await update.callback_query.answer(text, show_alert=True)
        return

    elif isinstance(error, (UnpicklingError, DecodingRedisDataError, TypeError, AttributeError)):
        text = (
            "⚠️ <b>Структура меню обновилась...</b>\n\n"
            "Начни заказ снова с помощью /start или обратись в тех. поддержку, если ошибка останется"
        )

    elif (
            isinstance(error, ConnectError)
            or (isinstance(error, NetworkError) and "ReadError" in error_msg)
    ):
        text = (
            "⚠️ <b>Что-то произошло с соединением...</b>\n\n"
            "Можешь повторить последнее действие\n\n"
            "Либо начни новый заказ — /start\n"
            "Если ошибка останется, обратись в тех. поддержку"
        )

    else:
        text = (
            "❌ <b>Произошла непредвиденная ошибка!</b>\n\n"
            "Попробуй повторить последнее действие, либо начни новый заказ с помощью /start, либо обратись в "
            "тех. поддержку с текстом ошибки\n\n"
            f"Текст ошибки:\n<pre>{error_type}: {error_msg}</pre>"
        )

    _ = await send_new_message(update, text, reply_markup, photo_name=None)
