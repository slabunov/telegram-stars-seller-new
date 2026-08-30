from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.callbacks import BackCallback, HistoryPageCallback, ProfileMenuCallback, create_callback
from bot.enums import BackDestination, ProfileAction


async def build_profile_kb(telegram_id: int, ) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📦 История заказов",
            callback_data=await create_callback(telegram_id, ProfileMenuCallback(ProfileAction.HISTORY))
        )],
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
