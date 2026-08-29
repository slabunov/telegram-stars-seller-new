class PayPearAPIError(Exception):
    """Базовая ошибка при работе с API PayPear."""


class PayPearAPINetworkError(PayPearAPIError):
    """Ошибка сети, когда запрос точно НЕ был отправлен."""
