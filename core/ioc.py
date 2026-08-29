import httpx
from typing import final
from collections.abc import Callable, Iterable, Awaitable

from dishka import AsyncContainer, Provider, Scope, provide, make_async_container
from dishka.integrations.base import wrap_injection

from core.integrations.fragment.client import FragmentClient, TIMEOUT as FRAGMENT_TIMEOUT, LIMITS as FRAGMENT_LIMITS
from core.integrations.paypear.client import PayPearClient, TIMEOUT as PAYPEAR_TIMEOUT, LIMITS as PAYPEAR_LIMITS
from core.integrations.platega.client import PlategaClient, TIMEOUT as PLATEGA_TIMEOUT, LIMITS as PLATEGA_LIMITS

from core.repositories.fragment_transaction import FragmentTransactionRepository
from core.repositories.payment import PaymentRepository
from core.repositories.promo_code import PromoCodeRepository
from core.repositories.transaction import TransactionRepository
from core.repositories.user import UserRepository

from core.services.fragment_transaction import FragmentTransactionService
from core.services.payment import PaymentService
from core.services.promo_code import PromoCodeService
from core.services.star_price import StarService
from core.services.stats import StatsService
from core.services.support import SupportService
from core.services.transaction import TransactionService
from core.services.user import UserService


@final
class BusinessLogicProvider(Provider):
    fragment_tx_repo = provide(FragmentTransactionRepository, scope=Scope.APP)
    payment_repo = provide(PaymentRepository, scope=Scope.APP)
    promo_repo = provide(PromoCodeRepository, scope=Scope.APP)
    trans_repo = provide(TransactionRepository, scope=Scope.APP)
    user_repo = provide(UserRepository, scope=Scope.APP)

    fragment_tx_service = provide(FragmentTransactionService,  scope=Scope.APP)
    payment_service = provide(PaymentService, scope=Scope.APP)
    promo_service = provide(PromoCodeService, scope=Scope.APP)
    star_service = provide(StarService, scope=Scope.APP)
    stats_service = provide(StatsService, scope=Scope.APP)
    support_service = provide(SupportService, scope=Scope.APP)
    trans_service = provide(TransactionService, scope=Scope.APP)
    user_service = provide(UserService, scope=Scope.APP)

    @provide(scope=Scope.APP)
    def platega_client(self) -> Iterable[PlategaClient]:
        with httpx.Client(timeout=PLATEGA_TIMEOUT, limits=PLATEGA_LIMITS) as client:
            yield PlategaClient(client)
            # Код после yield выполняется при вызове container.close()

    @provide(scope=Scope.APP)
    def paypear_client(self) -> Iterable[PayPearClient]:
        with httpx.Client(timeout=PAYPEAR_TIMEOUT, limits=PAYPEAR_LIMITS) as client:
            yield PayPearClient(client)
            # Код после yield выполняется при вызове container.close()

    @provide(scope=Scope.APP)
    def fragment_client(self, fragment_tx_service: FragmentTransactionService) -> Iterable[FragmentClient]:
        with httpx.Client(timeout=FRAGMENT_TIMEOUT, limits=FRAGMENT_LIMITS) as client:
            yield FragmentClient(client, fragment_tx_service)
            # Код после yield выполняется при вызове container.close()


# Глобальная переменная, которая будет хранить контейнер
# строго для текущего процесса ОС (Gunicorn, Celery-воркер и т.д.)
_container: AsyncContainer | None = None


def get_container() -> AsyncContainer:
    """Ленивая инициализация: контейнер создается только при первом обращении к нему."""
    global _container
    if _container is None:
        _container = make_async_container(BusinessLogicProvider())
    return _container


async def close_container() -> None:
    """
    Эту функцию нужно вызывать при завершении работы процесса,
    чтобы корректно закрыть все httpx-соединения и генераторы внутри провайдера.
    """
    global _container
    if _container is not None:
        await _container.close()
        _container = None


def inject[**P,R](func: Callable[P,Awaitable[R]]):
    """Данный декоратор можно использовать только с асинхронными функциями."""
    return wrap_injection(func=func, container_getter=lambda args, kwargs: get_container(), is_async=True)
