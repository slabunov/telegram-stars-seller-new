from enum import StrEnum

from core.domain.enums import TransactionStatus


class PayPearStatus(StrEnum):
    NEW = "NEW"
    PROCESS = "PROCESS"
    CONFIRMED = "CONFIRMED"
    CANCELED = "CANCELED"
    REFUNDED = "REFUNDED"
    EXPIRED = "EXPIRED"

    @staticmethod
    def transform_into_internal_status_or_keep_original(paypear_status: str) -> TransactionStatus | str:
        if paypear_status == PayPearStatus.CONFIRMED:
            return TransactionStatus.PROCESSING

        elif paypear_status in (PayPearStatus.CANCELED, PayPearStatus.EXPIRED):
            return TransactionStatus.CANCELLED

        elif paypear_status == PayPearStatus.REFUNDED:
            return TransactionStatus.CHARGEBACKED

        elif paypear_status in (PayPearStatus.NEW, PayPearStatus.PROCESS):
            return TransactionStatus.PENDING

        return paypear_status
