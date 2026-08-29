"""Быстрые проверки маппинга статусов и проброса контекста заказа через metadata PayPear."""

from core.domain.enums import TransactionStatus
from core.integrations.paypear.client import extract_payment_object
from core.integrations.paypear.enums import PayPearStatus
from core.integrations.paypear.schemas import build_paypear_metadata, parse_paypear_metadata
from core.integrations.platega.schemas import PaymentPayloadDict


_transform = PayPearStatus.transform_into_internal_status_or_keep_original


def test_status_mapping_covers_every_paypear_status():
    assert _transform("CONFIRMED") == TransactionStatus.PROCESSING
    assert _transform("CANCELED") == TransactionStatus.CANCELLED
    assert _transform("EXPIRED") == TransactionStatus.CANCELLED
    assert _transform("REFUNDED") == TransactionStatus.CHARGEBACKED
    assert _transform("NEW") == TransactionStatus.PENDING
    assert _transform("PROCESS") == TransactionStatus.PENDING
    # неизвестный статус возвращается как есть
    assert _transform("WAT") == "WAT"


def _payload(**overrides: object) -> PaymentPayloadDict:
    base: PaymentPayloadDict = {
        "user_id": 111,
        "message_id": 222,
        "price": 349.9,
        "stars_count": 250,
        "target_username": "someone",
        "payment_api": "PayPear",
        "pay_url": "",
        "promo_id": None,
        "promo_name": "",
        "promo_discount": None,
    }
    base.update(overrides)  # pyright: ignore[reportArgumentType]
    return base


def test_metadata_roundtrip_without_promo():
    payload = _payload()
    restored = parse_paypear_metadata(build_paypear_metadata(payload))
    assert restored == payload


def test_metadata_roundtrip_with_promo():
    payload = _payload(promo_id=7, promo_name="SUMMER", promo_discount="5.00")
    restored = parse_paypear_metadata(build_paypear_metadata(payload))
    assert restored == payload


def test_metadata_bad_input_returns_none():
    assert parse_paypear_metadata(None) is None
    assert parse_paypear_metadata({}) is None
    assert parse_paypear_metadata({"user_id": "not-a-number"}) is None
    assert parse_paypear_metadata({"only": "some", "keys": "here"}) is None


def test_extract_payment_object_accepts_both_wrapper_keys():
    obj = {"id": "x", "status": "NEW"}
    assert extract_payment_object({"success": True, "result": obj}) is obj  # pyright: ignore[reportArgumentType]
    assert extract_payment_object({"success": True, "response": obj}) is obj  # pyright: ignore[reportArgumentType]
    assert extract_payment_object({"success": False}) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all paypear checks passed")
