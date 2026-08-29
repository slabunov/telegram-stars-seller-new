"""Проверка порядка методов оплаты в боте: методы PayPear всегда наверху списка."""

from core.services.payment import _method_order_rank


def test_paypear_ranks_above_the_rest():
    assert _method_order_rank("PayPear") == 0
    assert _method_order_rank("PAYPEAR") == 0
    assert _method_order_rank("Platega") == 1


def test_sorted_floats_paypear_up_and_keeps_db_order_otherwise():
    methods = ["Platega", "Platega", "PayPear", "Platega", "PayPear"]
    ordered = sorted(methods, key=_method_order_rank)
    assert ordered == ["PayPear", "PayPear", "Platega", "Platega", "Platega"]
