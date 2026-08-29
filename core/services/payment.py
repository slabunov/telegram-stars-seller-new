import json
import logging
from decimal import Decimal
from typing import final, overload

from core.domain.enums import FINAL_MSG_STATUSES
from core.dto.payment import PaymentDTO, PaymentMethodDTO

from core.integrations.fragment.client import FragmentClient
from core.integrations.fragment.schemas import SendStarsResponse
from core.integrations.paypear.client import PayPearClient
from core.integrations.platega.client import PlategaClient
from core.integrations.platega.schemas import PaymentPayloadDict

from core.repositories.transaction import TransactionRepository
from core.repositories.user import UserRepository
from core.repositories.payment import PaymentRepository
from core.repositories.utils import db_action_with_tenacity

from core.services.star_price import StarService
from core.services.user import UnregisteredUser

from core.models import PaymentMethod, PromoCode, Transaction, TARGET_SELF


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("payment_audit")


def _method_order_rank(api_name: str) -> int:
    return 0 if "paypear" in api_name.lower() else 1


class NoUsernameError(Exception):
    """Исключение для случая, когда username отсутствует."""


class MaintenanceModeException(Exception):
    """Исключение для технического перерыва."""


@final
class PaymentService:
    def __init__(
            self,
            trans_repo: TransactionRepository,
            user_repo: UserRepository,
            payment_repo: PaymentRepository,
            star_service: StarService,
            platega_client: PlategaClient,
            paypear_client: PayPearClient,
            fragment_client: FragmentClient
    ):
        self._trans_repo = trans_repo
        self._user_repo = user_repo
        self._payment_repo = payment_repo
        self._star_service = star_service
        self._platega_client = platega_client
        self._paypear_client = paypear_client
        self._fragment_client = fragment_client

    async def ensure_no_maintenance_mode(self) -> None:
        if await self._payment_repo.is_maintenance_mode():
            raise MaintenanceModeException("maintenance_mode on True")

    async def get_active_payment_methods(self) -> tuple[PaymentMethodDTO, ...]:
        methods = sorted(
            await self._payment_repo.get_many_by(),
            key=lambda method: _method_order_rank(method.api.name)
        )
        return tuple(
            PaymentMethodDTO(
                api_name=method.api.name,
                name=method.name,
                external_id=method.external_id,
                commission_percent=method.commission_percent
            )
            for method in methods
        )

    async def get_payment_method(self, method_api: str, external_method_id: int | str) -> PaymentMethod | None:
        return await self._payment_repo.get_payment_method_by(method_api, external_method_id, is_check_is_active=False)

    async def create_payment(
            self,
            user_id: int, message_id: int,
            price: Decimal, stars_count: int, payment_api: str, method: int | str,
            target_username: str = "",
            promo: PromoCode | None = None
    ) -> tuple[PaymentDTO, PaymentPayloadDict]:
        """
        Обращается к внешнему API для создания платежа и получении ссылки на оплату, потом сохраняет транзакцию в БД.

        Возвращает PaymentDTO:

        `transaction_id` - UUID

        `pay_url` - str

        `price` - Decimal

        `expires_in` - str

        Arguments:

        - `user_id` - int, Telegram ID.

        - `message_id` - int, ID сообщения, для которого генерируется ссылка на оплату, чтобы вебхук мог изменить
        это сообщение.

        - `price` - Decimal, цена покупки.

        - `stars_count` - int, кол-во звёзд для перевода.

        - `payment_api` - str, API для создания платежа.

        - `method` - int | str, идентификатор метода оплаты во внешнем API (`PaymentMethod.external_id`).
        Для "Platega" это int (2 - СБП, 11 - Карточный эквайринг, 12 - Международный эквайринг, 13 - Криптовалюта),
        для "PayPear" - строковый `type` из ЛК (`sbp`, `card`, ...).
        В данный момент поддерживается только RUB.

        - `target_username` - str, по умолчанию "", если указан, то этому человеку будет сделан перевод звёзд.

        - `promo` - PromoCode | None, по умолчанию `None`, использованный промокод.
        """

        user_buyer = await db_action_with_tenacity(
            self._user_repo.get_by_telegram_id, user_id
        )
        if user_buyer is None:
            raise UnregisteredUser(user_id)

        payload_target_username = None
        if not target_username:
            if not user_buyer.username:
                raise NoUsernameError()

            payload_target_username = user_buyer.username

        description = f"For telegram user with ID {user_id}"

        if payload_target_username is None:
            payload_target_username = target_username

        payload: PaymentPayloadDict = {
            "user_id": user_id,
            "message_id": message_id,
            "price": float(price),
            "stars_count": stars_count,
            "target_username": payload_target_username,
            "payment_api": payment_api,
            "pay_url": "",
            "promo_id": None,
            "promo_name": "",
            "promo_discount": None
        }

        if promo is not None:
            payload["promo_id"] = promo.id
            payload["promo_name"] = promo.name
            payload["promo_discount"] = str(promo.discount)

        api_lower = payment_api.lower()

        if "platega" in api_lower:
            username = user_buyer.username
            if not username:
                if not target_username:
                    raise NoUsernameError("Для перевода должен быть username у покупателя или у получателя")
                username = f"отсутствует, но это подарок для {target_username}"

            payment_dto = await self._platega_client.create_payment(
                int(method),
                float(price), "RUB",
                description,
                str(user_id),
                username,
                payload=json.dumps(payload, ensure_ascii=False)
            )

        elif "paypear" in api_lower:
            payment_dto = await self._paypear_client.create_payment(
                str(method),
                float(price), "RUB",
                description,
                payload
            )

        else:
            raise NotImplementedError(f"Payment API '{payment_api}' is not supported")

        return payment_dto, payload

    @overload
    async def create_fragment_transaction(
            self,
            transaction: Transaction,
            *,
            timeout: float | None = None,
            recreate: bool = False
    ) -> SendStarsResponse | Transaction: ...

    @overload
    async def create_fragment_transaction(
            self,
            transaction: Transaction,
            *,
            timeout: float | None = None,
            recreate: bool = True
    ) -> SendStarsResponse: ...

    async def create_fragment_transaction(
            self,
            transaction: Transaction,
            *,
            timeout: float | None = None,
            connect: float | None = None,
            recreate: bool = False
    ) -> SendStarsResponse | Transaction:
        if not recreate:
            if transaction.status in FINAL_MSG_STATUSES:
                return transaction

        target_username = transaction.target_username
        if transaction.target_username == TARGET_SELF:
            target_username = transaction.telegram_user.username

        return await self._fragment_client.send_stars(
            target_username, transaction.amount_stars, transaction.id,
            timeout=timeout, connect=connect
        )
