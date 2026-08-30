from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.callbacks import BackCallback, HistoryPageCallback, create_callback
from bot.enums import BackDestination


async def build_profile_kb(telegram_id: int, ) -> InlineKeyboardMarkup:
    # Кнопка "📦 История заказов" скрыта: история показывает только транзакции в статусе SUCCESS,
    # а до него доходят не все оплаченные заказы. Вернуть, когда статусы будут доходить до SUCCESS.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "◀️ Назад",
            callback_data=await create_callback(telegram_id, BackCallback(BackDestination.MAIN_MENU))
        )]
    ])


async def build_order_history_kb(telegram_id: int, page: int, total_pages: int) -> InlineKeyboardMarkup:
    nav_buttons: list[InlineKeyboardButton] = []

    if page > 1:
        nav_buttons.append(InlineKeyboardButton(
            "⬅️ Назад",
            callback_data=await create_callback(telegram_id, HistoryPageCallback(page - 1))
        ))

    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(
            "Вперёд ➡️",
            callback_data=await create_callback(telegram_id, HistoryPageCallback(page + 1))
        ))

    keyboard: list[list[InlineKeyboardButton]] = []
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(
        "◀️ Назад в профиль",
        callback_data=await create_callback(telegram_id, BackCallback(BackDestination.PROFILE))
    )])

    return InlineKeyboardMarkup(keyboard)
