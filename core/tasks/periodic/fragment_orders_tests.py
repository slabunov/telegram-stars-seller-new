"""Проверки решения о статусе для опроса заказов Fragment."""

from datetime import timedelta

from core.domain.enums import TransactionStatus, is_change_status_allowed
from core.integrations.fragment.enums import FragmentStatus
from core.tasks.periodic.fragment_orders import WAITING_STATUSES, decide_next_status


_DOUBT_AFTER = timedelta(minutes=30)
_FRESH = timedelta(minutes=1)
_STALE = timedelta(hours=2)


def test_completed_order_closes_transaction():
    assert decide_next_status(FragmentStatus.COMPLETED, _FRESH, _DOUBT_AFTER) == TransactionStatus.SUCCESS
    assert decide_next_status(FragmentStatus.COMPLETED, _STALE, _DOUBT_AFTER) == TransactionStatus.SUCCESS


def test_failed_order_closes_transaction():
    assert decide_next_status(FragmentStatus.FAILED, _FRESH, _DOUBT_AFTER) == TransactionStatus.FAILED


def test_fresh_order_in_progress_is_left_alone():
    # пока заказ свежий, промежуточные статусы не должны трогать транзакцию
    for status in (FragmentStatus.CREATED, FragmentStatus.PENDING, FragmentStatus.BLOCKCHAIN_SENT):
        assert decide_next_status(status, _FRESH, _DOUBT_AFTER) is None


def test_stuck_order_goes_to_doubt():
    for status in (FragmentStatus.CREATED, FragmentStatus.PENDING, FragmentStatus.BLOCKCHAIN_SENT):
        assert decide_next_status(status, _STALE, _DOUBT_AFTER) == TransactionStatus.IN_DOUBT


def test_unknown_status_is_treated_as_in_progress_then_doubt():
    assert decide_next_status("WAT", _FRESH, _DOUBT_AFTER) is None
    assert decide_next_status("WAT", _STALE, _DOUBT_AFTER) == TransactionStatus.IN_DOUBT


def test_every_decision_is_an_allowed_transition_from_waiting_statuses():
    ages = (_FRESH, _STALE)
    for current_status in WAITING_STATUSES:
        for raw_status in FragmentStatus.all_enums():
            for age in ages:
                new_status = decide_next_status(raw_status, age, _DOUBT_AFTER)
                if new_status is None or new_status == current_status:
                    continue
                assert is_change_status_allowed(current_status, new_status), (
                    f"{current_status} -> {new_status} (fragment {raw_status}) запрещён матрицей переходов"
                )


def test_waiting_statuses_are_not_final_for_the_user():
    # транзакция в этих статусах ещё висит у пользователя как "Заказ обрабатывается..."
    assert TransactionStatus.SEND_CREATED in WAITING_STATUSES
    assert TransactionStatus.SUCCESS not in WAITING_STATUSES
    assert TransactionStatus.FAILED not in WAITING_STATUSES


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all fragment poll checks passed")
