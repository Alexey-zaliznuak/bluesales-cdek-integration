from external.cdek import Client

from external.bluesales import BlueSales


STATUSES = {
    "ЗАКАЗ ОФОРМЛЕН": "157154",
    "На макет": "165373",
    "Макет готов": "165375",
    "Правка макета": "165374",
    "На 2-й оплате": "161529",
    "Отложили заказ(2-я оплата)": "161923",
    "Макет гравировки": "167749",
    "Правка гравировки": "167750",
    "На изготовлении": "157220",
    "Сборка": "165332",
    "Гравировка заказа": "169165",
    "Правки заказа": "165578",
    "Заказ готов": "161547",
    "Ожидает отправку СДЭК": "157221",
    "Ожидает отправку ПОЧТА": "162603",
    "Упакован и ожидает отправку": "167654",
    "Отправлен+": "157223",
    "Ожидает в ПВЗ": "157222",
    "Доставлен": "157158",
    "Слет без предоплаты": "157175",
    "Разбор": "163495",
    "Слет с предоплатой": "157159",
    "Бронь": "158238",
    "Возврат": "160308"
}

CDEK_TO_CRM_STATUS_ID = {
    "CREATED": STATUSES["Заказ готов"],
    "ACCEPTED": STATUSES["ЗАКАЗ ОФОРМЛЕН"],
    "RECEIVED_AT_SHIPMENT_WAREHOUSE": STATUSES["Ожидает отправку СДЭК"],
    "READY_FOR_SHIPMENT_IN_SENDER_CITY": STATUSES["Ожидает отправку СДЭК"],
    "TAKEN_BY_TRANSPORTER_FROM_SENDER_CITY": STATUSES["Ожидает отправку СДЭК"],
    "SENT_TO_TRANSIT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_TRANSIT_CITY": STATUSES["Отправлен+"],
    "SENT_TO_RECIPIENT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_RECIPIENT_CITY": STATUSES["Ожидает в ПВЗ"],
    "OUT_FOR_DELIVERY": STATUSES["Доставлен"],
    "DELIVERED": STATUSES["Доставлен"],
    "NOT_DELIVERED": STATUSES["Разбор"],
    "RETURNED": STATUSES["Возврат"]
}


def get_crm_status_by_cdek(current_crm_status: str, cdek_status_name: str):
    return {
        ""
    }.get(cdek_status_name, current_crm_status)


def main():
    BLUESALES = BlueSales("managerYT13", "YTYT2025")
    CDEK = Client("XXsJ3TCwHbusuwgRNt6pgOeaq86Hj8o9", "lD1o1i6ZtyKiLxDyhuMHk52QxKoqwnxj")

    bluesales_orders = BLUESALES.orders.get_all()

    print("Всего:", len(bluesales_orders), "сделок")

    # отсеиваем у кого нет статуса или номера трекера в сдек
    bluesales_orders = list(filter(
        lambda o:
            o.tracking_number
            and o.status_name
            and o.status_name not in [
                "Возврат",
                "Доставлен",
            ],
        bluesales_orders
        )
    )

    print("Активных", len(bluesales_orders), "сделок")

    for order in bluesales_orders:
        try:
            cdek_status = CDEK.get_order_info(order.tracking_number)["entity"]["statuses"][0]["code"]
            print(order.status_name, cdek_status, get_crm_status_by_cdek(order.status_name, cdek_status))

        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
