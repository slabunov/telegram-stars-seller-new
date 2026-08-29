import asyncio
import httpx
import logging
from decimal import Decimal
from uuid import UUID, uuid4
from urllib.parse import urljoin
from datetime import datetime, timedelta, timezone
from typing import cast, final, NoReturn
from collections.abc import Mapping

from django.urls import reverse
from django.conf import settings

from core.domain.network_utils import SAFE_TO_RETRY
from core.dto.payment import PaymentDTO
from core.integrations.paypear.errors import PayPearAPIError, PayPearAPINetworkError
from core.integrations.paypear.schemas import (
    PayPearPaymentObjectJSON,
    PayPearPaymentRequestJSON,
    PayPearPaymentResponseJSON,
    build_paypear_metadata,
)
from core.integrations.platega.schemas import PaymentPayloadDict
from core.integrations.utils import create_new_timeout_conf_or_use_default


logger = logging.getLogger(__name__)


TIMEOUT = httpx.Timeout(timeout=15.0, connect=10.0)
LIMITS = httpx.Limits(max_keepalive_connections=10, keepalive_expiry=15.0)


PAYPEAR_WEBHOOK = "paypear_webhook"

DEFAULT_EXPIRES_IN = "00:30:00"
_PAYMENT_LIFETIME = timedelta(minutes=30)


def extract_payment_object(data: PayPearPaymentResponseJSON) -> PayPearPaymentObjectJSON | None:
    return data.get("result") or data.get("response")


@final
class PayPearClient:
    CREATE_PAYMENT_PATH = "payment/"
    ORDER_INFO_PATH = "payment/order/{order_id}/"

    def __init__(self, client: httpx.Client) -> None:
        self.url = cast(str, getattr(settings, "PAYPEAR_API_URL", None))  # noqa
        self.shop_id = cast(str, getattr(settings, "PAYPEAR_SHOP_ID", None))  # noqa
        self.secret = cast(str, getattr(settings, "PAYPEAR_SECRET", None))  # noqa
        self.site_domain = cast(str, getattr(settings, "SITE_DOMAIN", None))  # noqa
        self.return_url = cast(str, getattr(settings, "CHANNEL_LINK", None)) or self.site_domain  # noqa
        self.debug = cast(bool, getattr(settings, "DEBUG_PAYPEAR", False))  # noqa
        self.use_webhook = cast(bool, getattr(settings, "PAYPEAR_USE_WEBHOOK", True))  # noqa

        if not all([self.url, self.shop_id, self.secret, self.site_domain]):
            logger.error("PayPearClient не сконфигурирован.")
            raise ValueError("PayPearClient is not configured properly")

        self._client = client

    def build_webhook_url(self) -> str:
        return urljoin(self.site_domain, reverse(PAYPEAR_WEBHOOK))

    async def create_payment(
            self,
            method_type: str,
            amount: float,
            currency: str,
            description: str,
            payload: PaymentPayloadDict,
            *,
            timeout: float | None = None,
            connect: float | None = None
    ) -> PaymentDTO:
        """
        Создаёт платёж в PayPear. `order_id` (он же будущий ID транзакции) генерируем сами и
        отдаём обратно в `PaymentDTO.transaction_id`.

        `method_type` - значение `payment_method_data.type` из ЛК PayPear (`sbp`, `card`, ...),
        хранится в `PaymentMethod.external_id`.
        """
        order_id = uuid4()

        if self.debug:
            return PaymentDTO(
                transaction_id=order_id,
                pay_url="https://test.link",
                price=Decimal(str(amount)),
                expires_in=DEFAULT_EXPIRES_IN
            )

        expires_at = datetime.now(timezone.utc) + _PAYMENT_LIFETIME

        data: PayPearPaymentRequestJSON = {
            "order_id": str(order_id),
            "amount": {
                "value": f"{amount:.2f}",
                "currency": currency,
            },
            "confirmation": {
                "type": "redirect",
                "return_url": self.return_url,
            },
            "payment_method_data": {
                "type": method_type,
            },
            "description": description[:128],
            "metadata": build_paypear_metadata(payload),
            "expires_at": expires_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }
        if self.use_webhook:
            data["webhook_url"] = self.build_webhook_url()

        response = await self._make_request(
            "POST", self.CREATE_PAYMENT_PATH, str(order_id), data,
            timeout=timeout, connect=connect
        )

        if response.status_code == 200:
            response_data = cast(PayPearPaymentResponseJSON, response.json())
            payment_object = extract_payment_object(response_data)
            if payment_object is None:
                logger.exception(f"Неожиданный ответ PayPear без объекта платежа:\n{response_data = }")
                raise PayPearAPIError(f"PayPear не вернул объект платежа:\n{response_data = }")

            confirmation = payment_object.get("confirmation") or {}
            pay_url = confirmation.get("confirmation_url")

            return PaymentDTO(
                transaction_id=order_id,
                pay_url=pay_url,
                price=_parse_amount(payment_object, fallback=amount),
                expires_in=_parse_expires_in(payment_object, fallback=expires_at)
            )

        _raise_for_error(response, data)

    async def get_payment_info(
            self,
            order_id: UUID | str,
            *,
            timeout: float | None = None,
            connect: float | None = None
    ) -> PayPearPaymentObjectJSON | None:
        """
        Актуальное состояние платежа по нашему `order_id`.

        Используется задачей опроса `poll_pending_paypear_payments_task`, когда webhook-и
        отключены, а также годится как способ проверки неподписанных вебхуков PayPear.
        """
        if self.debug:
            return None

        path = self.ORDER_INFO_PATH.format(order_id=order_id)
        response = await self._make_request("GET", path, str(order_id), timeout=timeout, connect=connect)

        if response.status_code == 200:
            response_data = cast(PayPearPaymentResponseJSON, response.json())
            return extract_payment_object(response_data)

        if response.status_code == 404:
            return None

        _raise_for_error(response, {"order_id": str(order_id)})

    async def _make_request(
            self,
            method: str,
            path: str,
            idempotence_key: str,
            data: Mapping[str, object] | None = None,
            *,
            timeout: float | None = None,
            connect: float | None = None
    ) -> httpx.Response:
        full_url = urljoin(self.url, path)
        timeout_conf = create_new_timeout_conf_or_use_default(timeout, connect, TIMEOUT)
        auth = (self.shop_id, self.secret)

        def do_sync_request() -> httpx.Response:
            if method == "POST":
                return self._client.post(
                    full_url,
                    json=data, auth=auth, timeout=timeout_conf,
                    headers={"Idempotence-Key": idempotence_key, "Content-Type": "application/json"},
                )

            return self._client.get(full_url, auth=auth, timeout=timeout_conf)

        try:
            response = await asyncio.to_thread(do_sync_request)

        except (*SAFE_TO_RETRY, ) as exc:
            err_msg = "Произошла ошибка соединения при обращении к PayPear"
            logger.exception(err_msg)
            raise PayPearAPINetworkError(err_msg) from exc

        except httpx.TimeoutException as exc:
            logger.exception("Превышено время ожидания при обращении к PayPear")
            raise PayPearAPIError("Превышено время ожидания при обращении к PayPear") from exc

        except httpx.HTTPError as exc:
            logger.exception(f"Ошибка HTTP при обращении к PayPear: {exc}")
            raise PayPearAPIError(f"Ошибка HTTP при обращении к PayPear: {exc}") from exc

        return response


def _parse_amount(payment_object: PayPearPaymentObjectJSON, *, fallback: float) -> Decimal:
    amount = payment_object.get("amount") or {}
    value = amount.get("value")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(str(fallback))


def _parse_expires_in(payment_object: PayPearPaymentObjectJSON, *, fallback: datetime) -> str:
    raw = payment_object.get("expires_at")
    expires_at = fallback
    if raw:
        try:
            expires_at = datetime.fromisoformat(raw)
        except ValueError:
            pass

    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    remaining = expires_at - datetime.now(timezone.utc)
    total_seconds = int(remaining.total_seconds())
    # дальше по коду expires_in парсится через strptime("%H:%M:%S"), где %H не принимает >23,
    # поэтому за пределами суток отдаём безопасное значение по умолчанию
    if total_seconds <= 0 or total_seconds >= 24 * 3600:
        return DEFAULT_EXPIRES_IN

    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _raise_for_error(response: httpx.Response, request_data: Mapping[str, object]) -> NoReturn:
    try:
        body = cast(PayPearPaymentResponseJSON, response.json())
        error = body.get("error") or {}
        detail = f"{error.get('code')}: {error.get('message')}"
    except Exception:
        detail = response.text[:500]

    if response.status_code in (401, 403):
        logger.exception(f"Не удалось авторизоваться в PayPear: {detail}")
        raise PayPearAPIError(f"Не удалось авторизоваться в PayPear: {detail}")

    if response.status_code == 400:
        logger.exception(f"Ошибка валидации при обращении к PayPear: {detail}\n{request_data = }")
        raise PayPearAPIError(f"Ошибка валидации при обращении к PayPear: {detail}")

    logger.exception(f"Неизвестная ошибка PayPear ({response.status_code}): {detail}\n{request_data = }")
    raise PayPearAPIError(f"Неизвестная ошибка PayPear ({response.status_code}): {detail}")
