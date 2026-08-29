import asyncio
from httpx import Timeout
from collections.abc import Callable, Awaitable

from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from core.domain.tenacity_utils import RetryConfig
from core.integrations.fragment.errors import (
    FragmentAPINetworkError,
    FragmentAPITemporaryError,
    FragmentAPITooManyRequests
)
from core.integrations.paypear.errors import PayPearAPINetworkError
from core.integrations.platega.errors import PlategaAPINetworkError


def create_new_timeout_conf_or_use_default(timeout: float | None, connect: float | None, default: Timeout) -> Timeout:
    if timeout is not None and connect is not None:
        return Timeout(timeout=timeout, connect=connect)

    if timeout is not None:
        return Timeout(timeout=timeout)

    if connect is not None:
        return Timeout(timeout=default.read, connect=connect)

    return default


async def retries_with_tenacity[**P,R](func: Callable[P,Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
    retry_config = RetryConfig()

    async for attempt in AsyncRetrying(
            stop=stop_after_attempt(retry_config.attempts),
            wait=wait_exponential_jitter(
                initial=retry_config.initial_wait,
                max=retry_config.max_wait,
                jitter=retry_config.jitter
            ),
            retry=retry_if_exception_type((
                    FragmentAPINetworkError,
                    FragmentAPITemporaryError,
                    FragmentAPITooManyRequests,
                    PlategaAPINetworkError,
                    PayPearAPINetworkError
            )),
            reraise=True
    ):
        with attempt:
            try:
                return await func(*args, **kwargs)

            except FragmentAPITooManyRequests as err:
                time_to_sleep = float(err.retry_after) if err.retry_after is not None else 10.0
                time_to_sleep = time_to_sleep - attempt.retry_state.upcoming_sleep + 1
                if time_to_sleep > 0:
                    await asyncio.sleep(time_to_sleep)
                raise err

    return await func(*args, **kwargs)
