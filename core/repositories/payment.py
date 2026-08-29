from decimal import Decimal

from core.dto.payment import PricingDTO
from core.models import GlobalSettings, ExchangeRate, PaymentMethod


class PaymentRepository:
    model_settings: type[GlobalSettings] = GlobalSettings
    model_exchange_rate: type[ExchangeRate] = ExchangeRate
    model_payment_method: type[PaymentMethod] = PaymentMethod

    async def get_pricing_data(self) -> PricingDTO:
        """Объединение двух SQL-запросов в один поток для производительности."""
        return PricingDTO(
            settings=await self.model_settings.aget_solo(),
            exchange_rate=await self.model_exchange_rate.aget_solo()
        )

    async def get_payment_method_by(
            self,
            method_api: str,
            external_method_id: int | str | None = None,
            method_name: str | None = None,
            is_check_is_active: bool = True,
            is_active_value: bool = True
    ) -> PaymentMethod | None:
        if external_method_id is None and method_name is None:
            return None

        query = (
            self.model_payment_method.objects
            .select_related("api")
            .filter(api__name=method_api)
        )

        if external_method_id is not None:
            query = query.filter(external_id=external_method_id)

        if method_name is not None:
            query = query.filter(name=method_name)

        if is_check_is_active:
            query = query.filter(is_active=is_active_value)

        return await query.afirst()

    async def get_many_by(
            self,
            is_check_is_active: bool = True,
            is_active_value: bool = True,
            is_select_api: bool = True
    ) -> list[PaymentMethod]:
        """По умолчанию возвращает активные методы оплаты для отображения в боте."""
        query = self.model_payment_method.objects

        if is_check_is_active:
            query = query.filter(is_active=is_active_value)

        if is_select_api:
            query = query.select_related("api")

        return [method async for method in query.all()]

    async def update_current_usd_rate(self, current_usd_rate: Decimal) -> None:
        """Метод для сохранения текущего курса доллара при его обновлении по таймеру."""
        exchange_rate = await self.model_exchange_rate.aget_solo()
        exchange_rate.usd_rate = current_usd_rate
        await exchange_rate.asave(update_fields=["usd_rate", "updated_at"])

    async def is_maintenance_mode(self) -> bool:
        settings = await self.model_settings.aget_solo()
        return settings.maintenance_mode
