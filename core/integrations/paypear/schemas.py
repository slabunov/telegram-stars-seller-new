from uuid import UUID
from typing import NotRequired, TypedDict, Annotated, cast

# PaymentPayloadDict / PaymentPayloadValidateModel живут в platega.schemas и являются
# общими для всех платёжных интеграций.
from core.integrations.platega.schemas import PaymentPayloadDict, PaymentPayloadValidateModel


class PayPearAmountJSON(TypedDict):
    value: str
    currency: str


class PayPearConfirmationRequestJSON(TypedDict):
    type: str
    return_url: str


class PayPearPaymentMethodDataJSON(TypedDict):
    type: str


class PayPearPaymentRequestJSON(TypedDict):
    order_id: str
    amount: PayPearAmountJSON
    confirmation: PayPearConfirmationRequestJSON
    payment_method_data: PayPearPaymentMethodDataJSON
    description: str
    metadata: dict[str, str]
    webhook_url: NotRequired[str]
    expires_at: NotRequired[str]


class PayPearConfirmationResponseJSON(TypedDict):
    type: str
    confirmation_url: str | None
    return_url: NotRequired[str | None]


class PayPearPaymentObjectJSON(TypedDict):
    id: Annotated[str, UUID]
    shop_id: int
    order_id: Annotated[str, UUID]
    status: str
    description: NotRequired[str]
    amount: PayPearAmountJSON
    credited_amount: NotRequired[PayPearAmountJSON]
    confirmation: NotRequired[PayPearConfirmationResponseJSON | None]
    created_at: str
    expires_at: NotRequired[str | None]
    paid: bool
    metadata: NotRequired[dict[str, str]]
    webhook_url: NotRequired[str]


class PayPearErrorJSON(TypedDict):
    status_code: int
    code: str
    message: str


class PayPearPaymentResponseJSON(TypedDict):
    success: bool
    # В документации ключ называется то "result", то "response" - обрабатываем оба.
    result: NotRequired[PayPearPaymentObjectJSON]
    response: NotRequired[PayPearPaymentObjectJSON]
    error: NotRequired[PayPearErrorJSON]


class PayPearWebhookRequestJSON(TypedDict):
    type: str
    event: str
    object: PayPearPaymentObjectJSON
    signature: str


# Проброс контекста заказа через metadata платежа.
# У Platega для этого есть строковый "payload"; у PayPear - "metadata" (строковые
# пары "ключ-значение", максимум 16 ключей, значение не длиннее 512 символов).
# Наш PaymentPayloadDict имеет 10 коротких полей, раскладываем его в metadata и
# собираем обратно из вебхука.

_PAYLOAD_INT_KEYS = frozenset(("user_id", "message_id", "stars_count"))
_PAYLOAD_NULLABLE_INT_KEYS = frozenset(("promo_id",))
_PAYLOAD_FLOAT_KEYS = frozenset(("price",))
_PAYLOAD_NULLABLE_STR_KEYS = frozenset(("promo_discount",))


def build_paypear_metadata(payload: PaymentPayloadDict) -> dict[str, str]:
    """Плоское строковое представление PaymentPayloadDict для поля metadata платежа."""
    return {key: "" if value is None else str(value) for key, value in payload.items()}


def parse_paypear_metadata(metadata: dict[str, str] | None) -> PaymentPayloadDict | None:
    """Собирает PaymentPayloadDict обратно из metadata вебхука. None, если данных не хватает или они невалидны."""
    if not metadata:
        return None

    try:
        restored: dict[str, object] = {}
        for key, raw in metadata.items():
            if key in _PAYLOAD_INT_KEYS:
                restored[key] = int(raw)
            elif key in _PAYLOAD_NULLABLE_INT_KEYS:
                restored[key] = int(raw) if raw != "" else None
            elif key in _PAYLOAD_FLOAT_KEYS:
                restored[key] = float(raw)
            elif key in _PAYLOAD_NULLABLE_STR_KEYS:
                restored[key] = raw if raw != "" else None
            else:
                restored[key] = raw

        _ = PaymentPayloadValidateModel(**restored)  # pyright: ignore[reportArgumentType]
    except Exception:
        return None

    return cast(PaymentPayloadDict, restored)
