from django.urls import path

from core.views import payment_webhook, paypear_webhook, fragment_webhook, test_webhook, health
from core.integrations.fragment.client import FRAGMENT_WEBHOOK
from core.integrations.paypear.client import PAYPEAR_WEBHOOK
from core.integrations.platega.client import PLATEGA_WEBHOOK


urlpatterns = [
    path("webhooks/platega/", payment_webhook, name=PLATEGA_WEBHOOK),
    path("webhooks/paypear/", paypear_webhook, name=PAYPEAR_WEBHOOK),
    path("webhooks/fragment/", fragment_webhook, name=FRAGMENT_WEBHOOK),
    path("webhooks/test/", test_webhook, name="test_webhook"),
    path("health/", health, name="health"),
]
