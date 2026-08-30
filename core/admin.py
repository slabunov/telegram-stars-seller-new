import json
from decimal import Decimal
from typing import final, override, cast
from collections.abc import Mapping

from django import forms
from django.conf import settings
from django.contrib import admin, messages
from django.http import HttpRequest, HttpResponseRedirect
from django.db.models import Sum
from django.db.models.functions import TruncMonth

from solo.admin import SingletonModelAdmin

from core.forms import BroadcastForm
from core.models import (
    FragmentTransaction, PaymentAPI, PromoCode, TelegramUser, Transaction, TransactionMetadata,
    PaymentMethod, GlobalSettings, FragmentAPI,
    MonthlyProfit, Broadcast
)
from core.services.redis_service import publish_broadcast_task


class TransactionMetadataMixin:
    """Миксин для названий полей из связанной модели метаданных транзакции."""
    def transaction_type(self, obj: Transaction) -> str:
        try:
            return obj.metadata_info.get_type_display()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
        except TransactionMetadata.DoesNotExist:
            return "—"
    transaction_type.short_description = "Тип"  # pyright: ignore[reportFunctionMemberAccess]
    transaction_type.admin_order_field = "metadata_info__type"  # pyright: ignore[reportFunctionMemberAccess]

    def promo_id(self, obj: Transaction) -> int | None | str:
        try:
            return obj.metadata_info.promo_id
        except TransactionMetadata.DoesNotExist:
            return "—"
    promo_id.short_description = "ID промо"  # pyright: ignore[reportFunctionMemberAccess]
    promo_id.admin_order_field = "metadata_info__promo_id"  # pyright: ignore[reportFunctionMemberAccess]

    def promo_name(self, obj: Transaction) -> str:
        try:
            return obj.metadata_info.promo_name
        except TransactionMetadata.DoesNotExist:
            return "—"
    promo_name.short_description = "Имя промо"  # pyright: ignore[reportFunctionMemberAccess]
    promo_name.admin_order_field = "metadata_info__promo_name"  # pyright: ignore[reportFunctionMemberAccess]

    def promo_discount(self, obj: Transaction) -> Decimal | None | str:
        try:
            return obj.metadata_info.promo_discount
        except TransactionMetadata.DoesNotExist:
            return "—"
    promo_discount.short_description = "Скидка% промо"  # pyright: ignore[reportFunctionMemberAccess]
    promo_discount.admin_order_field = "metadata_info__promo_discount"  # pyright: ignore[reportFunctionMemberAccess]



@final
class TransactionInline(admin.TabularInline, TransactionMetadataMixin):  # pyright: ignore[reportMissingTypeArgument]
    """Инлайн для отображения транзакций в карточке пользователя"""
    model: type[Transaction] = Transaction
    readonly_fields = (
        "id", "target_username", "amount_stars", "amount_fiat", "status", "transaction_type",
        "promo_name", "promo_discount", "promo_id",
        "created_at", "expires_at", "updated_at"
    )
    show_change_link = True
    can_delete = False
    ordering = ("-created_at",)
    verbose_name = "История транзакций"
    verbose_name_plural = "Истории транзакций"

    @override
    def has_add_permission(self, request: HttpRequest, obj: object | None = None):  return False


@final
class TelegramUserInline(admin.TabularInline):  # pyright: ignore[reportMissingTypeArgument]
    model: type[TelegramUser] = TelegramUser
    readonly_fields = (
        "username", "telegram_id", "promo_since", "is_active", "created_at", "updated_at"
    )
    show_change_link = True
    can_delete = False
    ordering = ("-promo_since",)
    verbose_name = "Список воспользовавшихся пользователей"
    verbose_name_plural = "Списки воспользовавшихся пользователей"

    @override
    def has_add_permission(self, request: HttpRequest, obj: object | None = None):  return False


@final
@admin.register(PromoCode)
class PromoCodeAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    list_display = (
        "name", "discount", "usage_global", "usage_account", "is_active", "created_at", "updated_at"
    )
    ordering = ("-created_at", )
    search_fields = ("name", "discount", "usage_global", "usage_account")
    search_help_text = "Поиск по имени, скидке и использованиям"
    list_filter = (
        "is_active", ("created_at", admin.DateFieldListFilter), ("updated_at", admin.DateFieldListFilter)
    )
    readonly_fields = ("created_at", "updated_at")
    inlines = [TelegramUserInline]


@final
@admin.register(TelegramUser)
class TelegramUserAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    list_display = (
        "username", "telegram_id", "active_promo", "promo_since", "is_active", "created_at", "updated_at"
    )
    ordering = ("-created_at", )
    search_fields = ("username", "telegram_id", "active_promo__name")
    search_help_text = "Поиск по имени пользователя или ID и имени промокода"
    list_filter = (
        ("created_at", admin.DateFieldListFilter), ("updated_at", admin.DateFieldListFilter),
        ("promo_since", admin.DateFieldListFilter), "is_active"
    )
    if settings.DEBUG:  # pyright: ignore[reportAny]
        readonly_fields = ("promo_since", "created_at", "updated_at")
    else:
        readonly_fields = (
            "username", "telegram_id", "promo_since", "created_at", "updated_at"
        )
    inlines = [TransactionInline]


class PrettyJSONWidget(forms.Textarea):
    @override
    def format_value(self, value: str | Mapping[str, object]):
        try:
            if isinstance(value, str):
                value = cast(dict[str, object], json.loads(value))
            return json.dumps(value, indent=4, ensure_ascii=False)

        except (ValueError, TypeError):
            return super().format_value(value)


@final
class TransactionMetadataInline(admin.StackedInline):  # pyright: ignore[reportMissingTypeArgument]
    model: type[TransactionMetadata] = TransactionMetadata
    can_delete = False
    formfield_overrides = {
        TransactionMetadata._meta.get_field(
            "payload"
        ).__class__: {"widget": PrettyJSONWidget}
    }

    @override
    def has_change_permission(self, request: HttpRequest, obj: object | None = None): return False


@final
@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin, TransactionMetadataMixin):  # pyright: ignore[reportMissingTypeArgument]
    list_display = (
        "id", "telegram_user", "amount_stars", "amount_fiat", "target_username", "status", "transaction_type",
        "promo_name", "promo_discount", "promo_id",
        "created_at", "expires_at", "updated_at"
    )
    ordering = ("-created_at", )
    list_filter = (
        "status", ("created_at", admin.DateFieldListFilter), ("updated_at", admin.DateFieldListFilter),
        ("expires_at", admin.DateFieldListFilter)
    )
    search_fields = (
        "id", "telegram_user__username", "telegram_user__telegram_id", "metadata_info__promo_name"
    )
    search_help_text = "Поиск по ID транзакции, имени пользователя или ID и имени промокода"
    readonly_fields = ("created_at", "expires_at", "updated_at")
    readonly_fields_when_created = ("id",)
    inlines = [TransactionMetadataInline]

    @override
    def get_readonly_fields(self, request: HttpRequest, obj: Transaction | None = None):  # pyright: ignore[reportUnknownParameterType]
        if obj:
            return tuple(list(self.readonly_fields_when_created) + list(self.readonly_fields))  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportUnknownArgumentType]

        return tuple(self.readonly_fields)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportUnknownArgumentType]


@final
class PaymentMethodInline(admin.TabularInline):  # pyright: ignore[reportMissingTypeArgument]
    model: type[PaymentMethod] = PaymentMethod
    fields = ("name", "commission_percent", "external_id", "is_active")
    extra = 0


@final
@admin.register(PaymentAPI)
class PaymentAPIAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    inlines = [PaymentMethodInline]
    list_display = ("name", )


@final
@admin.register(FragmentTransaction)
class FragmentTransactionAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    list_display = (
        "fragment_id", "id_from_payment_api", "status", "created_at", "updated_at"
    )
    ordering = ("-created_at", )
    search_fields = ("fragment_id", "id_from_payment_api")
    search_help_text = "Поиск по обоим ID"
    list_filter = (
        "status", ("created_at", admin.DateFieldListFilter), ("updated_at", admin.DateFieldListFilter)
    )
    readonly_fields = ("created_at", "updated_at")
    readonly_fields_when_created = ("fragment_id", "id_from_payment_api")

    @override
    def get_readonly_fields(self, request: HttpRequest, obj: Transaction | None = None):  # pyright: ignore[reportUnknownParameterType]
        if obj:
            return tuple(list(self.readonly_fields_when_created) + list(self.readonly_fields))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType, reportUnknownMemberType]

        return tuple(self.readonly_fields)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportUnknownArgumentType]


@final
@admin.register(GlobalSettings)
class GlobalSettingsAdmin(SingletonModelAdmin):
    # Добавляем отображение полей из ExchangeRate прямо сюда с помощью "виртуальных" полей
    # readonly_fields = ("usd_current_rate_display", "last_usd_rate_update_display")
    readonly_fields = ("updated_at", )

    fieldsets = (
        ("Основные настройки цены", {
            "fields": ("star_base_cost", )
        }),
        # ("Основные настройки цены", {
        #     "fields": ("star_base_cost", "usd_base_rate", "is_use_usd_rate")
        # }),
        # ("Текущие рыночные данные", {
        #     "fields": ("usd_current_rate_display", "last_usd_rate_update_display"),
        #     "description": "Эти данные обновляются автоматически и используются для расчета, если включена опция выше."
        # }),
        ("Статус бота", {
            "fields": ("maintenance_mode", ),
        }),
        ("Общее", {
            "fields": ("updated_at", )
        })
    )

    # def usd_current_rate_display(self, obj: object) -> Decimal:
    #     return ExchangeRate.get_solo().usd_rate
    #
    # usd_current_rate_display.short_description = "Текущий курс доллара"
    #
    # def last_usd_rate_update_display(self, obj: object) -> str:
    #     return localize(timezone.template_localtime(ExchangeRate.get_solo().updated_at))
    #
    # last_usd_rate_update_display.short_description = "Дата последнего обновления курса"


@final
@admin.register(MonthlyProfit)
class MonthlyProfitAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    # Кастомный шаблон
    change_list_template = "admin/monthly_profit_report.html"

    @override
    def changelist_view(self, request: HttpRequest, extra_context: dict[str, str] | None = None):
        monthly_stats = (
            Transaction.objects
            .filter(status="SUCCESS")
            .annotate(month=TruncMonth("created_at"))
            .values("month")
            .annotate(monthly_profit=Sum("amount_fiat"))
            .order_by("-month")
        )

        total_all_time = (
            Transaction.objects
            .filter(status="SUCCESS")
            .aggregate(total_profit=Sum("amount_fiat"))["total_profit"] or 0
        )

        extra_context = extra_context or {}
        extra_context["title"] = "Отчет по прибыли по месяцам"
        extra_context["monthly_stats"] = monthly_stats  # noqa  # pyright: ignore[reportArgumentType]
        extra_context["total_all_time"] = total_all_time  # pyright: ignore[reportArgumentType]

        return super().changelist_view(request, extra_context=extra_context)

    # Запрещаем любые действия, кроме просмотра
    @override
    def has_add_permission(self, request: HttpRequest): return False
    @override
    def has_delete_permission(self, request: HttpRequest, obj: object | None = None): return False
    @override
    def has_change_permission(self, request: HttpRequest, obj: object | None = None): return False


@final
@admin.register(FragmentAPI)
class FragmentAPIAdmin(SingletonModelAdmin):
    list_display = ("token", "updated_at")
    readonly_fields = ("updated_at", )


@final
@admin.register(Broadcast)
class BroadcastAdmin(admin.ModelAdmin):  # pyright: ignore[reportMissingTypeArgument]
    form = BroadcastForm
    change_form_template = "admin/broadcast_change_form.html"
    readonly_fields = ("telegram_file_id", "preview_sent", "is_sent", "created_at")

    @override
    def response_change(self, request: HttpRequest, obj: Broadcast | None):
        if "_send_preview" in request.POST:
            publish_broadcast_task("preview", obj.id)
            messages.info(request, "Предпросмотр отправляется... Обновите страницу через пару секунд.")
            return HttpResponseRedirect(request.path)

        if "_send_broadcast" in request.POST:
            if not obj.preview_sent:
                err_msg = (
                    "Ошибка: Сначала необходимо отправить предпросмотр! "
                    "(Если вы изменили медиа, предпросмотр нужно отправить заново)."
                )
                messages.error(request, err_msg)
                return HttpResponseRedirect(request.path)

            if obj.media and not obj.telegram_file_id:
                err_msg = (
                    "Ошибка: Медиафайл прикреплен, но file_id не получен. "
                    "Отправьте предпросмотр еще раз, чтобы Telegram загрузил файл."
                )
                messages.error(request, err_msg)
                return HttpResponseRedirect(request.path)

            publish_broadcast_task("broadcast", obj.id)

            if obj.is_sent:
                messages.error(request, "ПОВТОРНАЯ массовая рассылка запущена в фоновом режиме.")
            else:
                messages.success(request, "Массовая рассылка запущена в фоновом режиме.")

            return HttpResponseRedirect(request.path)

        return super().response_change(request, obj)  # pyright: ignore[reportUnknownMemberType]
