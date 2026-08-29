from django.db import migrations


PAYPEAR = "PayPear"


def create_paypear_api(apps, schema_editor):
    payment_api = apps.get_model("core", "paymentapi")
    _ = payment_api.objects.get_or_create(name=PAYPEAR)


def remove_paypear_api(apps, schema_editor):
    payment_api = apps.get_model("core", "paymentapi")
    _ = payment_api.objects.filter(name=PAYPEAR).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0019_add_is_active_to_user"),
    ]

    operations = [
        migrations.RunPython(create_paypear_api, remove_paypear_api),
    ]
