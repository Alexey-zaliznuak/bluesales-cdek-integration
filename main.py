from external.cdek import Client

from external.bluesales import BlueSales


def main():
    BLUESALES = BlueSales("managerYT13", "YTYT2025")
    print(BLUESALES.orders.get_statuses())
    CDEK = Client("XXsJ3TCwHbusuwgRNt6pgOeaq86Hj8o9", "lD1o1i6ZtyKiLxDyhuMHk52QxKoqwnxj")

    bluesales_orders = BLUESALES.orders.get_all()

    print("Всего:", len(bluesales_orders), "заказов")

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

    print("Активных", len(bluesales_orders), "заказов")

    for order in bluesales_orders:
        cdek_status = CDEK.get_order_info("10069444227")["entity"]["statuses"][0]["code"]
        print(order.id, order.status_name, cdek_status)

if __name__ == "__main__":
    main()