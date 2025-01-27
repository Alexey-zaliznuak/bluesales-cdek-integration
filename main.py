# !/home/admin/update-statuses-by-transfering/venv/bin/python
import os
import vk_api

from typing import List, Tuple

from datetime import datetime
from time import sleep
from requests.exceptions import HTTPError

from external.cdek import Client
from external.bluesales import BlueSales
from external.bluesales.exceptions import BlueSalesError

import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from settings import INVERTED_STATUSES, STATUSES, VK_CLIENTS_BY_GROUP_ID
from external.bluesales.ordersapi import Order


logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler("/home/admin/update-statuses-by-transfering/log.log", maxBytes=64*1024, backupCount=3, encoding='utf-8')
formatter = logging.Formatter('%(message)s')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

full_file_handler = RotatingFileHandler("/home/admin/update-statuses-by-transfering/full_log.log", maxBytes=256*1024, backupCount=3, encoding='utf-8')
full_file_handler.setFormatter(formatter)
full_file_handler.setLevel(logging.DEBUG)

logger.addHandler(file_handler)
logger.addHandler(full_file_handler)

stream_handler = StreamHandler()
stream_formatter = logging.Formatter("%(message)s")
stream_handler.setFormatter(stream_formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)


CDEK_TO_CRM_STATUS_ID = {
    # "ACCEPTED": STATUSES["ЗАКАЗ ОФОРМЛЕН"],
    "CREATED": STATUSES["Отправлен+"],
    "RECEIVED_AT_SHIPMENT_WAREHOUSE": STATUSES["Отправлен+"],
    "READY_TO_SHIP_AT_SENDING_OFFICE": STATUSES["Отправлен+"],
    "READY_FOR_SHIPMENT_IN_TRANSIT_CITY": STATUSES["Отправлен+"],
    "READY_FOR_SHIPMENT_IN_SENDER_CITY": STATUSES["Отправлен+"],
    "RETURNED_TO_SENDER_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "TAKEN_BY_TRANSPORTER_FROM_SENDER_CITY": STATUSES["Отправлен+"],
    "SENT_TO_TRANSIT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_TRANSIT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_AT_TRANSIT_WAREHOUSE": STATUSES["Отправлен+"],
    "RETURNED_TO_TRANSIT_WAREHOUSE": STATUSES["Отправлен+"],
    "READY_TO_SHIP_IN_TRANSIT_OFFICE": STATUSES["Отправлен+"],
    "TAKEN_BY_TRANSPORTER_FROM_TRANSIT_CITY": STATUSES["Отправлен+"],
    "SENT_TO_SENDER_CITY": STATUSES["Отправлен+"],
    "SENT_TO_RECIPIENT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_SENDER_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_RECIPIENT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_AT_RECIPIENT_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "ACCEPTED_AT_PICK_UP_POINT": STATUSES["Ожидает в ПВЗ"],
    "TAKEN_BY_COURIER": STATUSES["Отправлен+"],
    "RETURNED_TO_RECIPIENT_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "DELIVERED": STATUSES["Доставлен"],
    "NOT_DELIVERED": STATUSES["Возврат"],
    "INVALID": STATUSES["Возврат"],
    "IN_CUSTOMS_INTERNATIONAL": STATUSES["Отправлен+"],
    "SHIPPED_TO_DESTINATION": STATUSES["Отправлен+"],
    "PASSED_TO_TRANSIT_CARRIER": STATUSES["Отправлен+"],
    "IN_CUSTOMS_LOCAL": STATUSES["Отправлен+"],
    "CUSTOMS_COMPLETE": STATUSES["Ожидает в ПВЗ"],
    "POSTOMAT_POSTED": STATUSES["Ожидает в ПВЗ"],
    "POSTOMAT_SEIZED": STATUSES["Возврат"],
    "POSTOMAT_RECEIVED": STATUSES["Доставлен"],
}



text_for_pvz = (
    "Здравствуйте!\n"
    "Ваши часы ожидают в пункте выдачи!❤️\n\n"
    "Обратите внимание, что в заказе Вас ждёт подарок!🎁\n\n"
    "Вы мне очень сильно поможете, если заберете заказ СЕГОДНЯ🙏🏻\n"
    "Просто отчетность о доставленных заказах напрямую влияет на мою зарплату😔 "
    "Буду Вам очень благодарна😊\n"
    "Сегодня получится забрать заказ?)"
)
text_for_postomat = (
    "Здравствуйте!\n"
    "Ваши часы ожидают в постамате!❤️\n\n"
    "Обратите внимание, что СРОК ХРАНЕНИЯ 2 ДНЯ и в заказе Вас ждёт подарок!🎁\n\n"
    "Вы мне очень сильно поможете, если заберете заказ СЕГОДНЯ🙏🏻\n"
    "Просто отчетность о доставленных заказах напрямую влияет на мою зарплату😔 "
    "Буду Вам очень благодарна😊\n"
    "Сегодня получится забрать заказ?)"
)

def notify_that_orders_in_pvz(orders: List[Tuple[Order, bool]]):
    # orders: List[Order, is_postomat: bool]
    if not orders:
        return

    logger.info("\n=== Рассылка уведомления о доставке в пунты выдачи / постаматы ===")

    for order in orders:
        order, is_postomat = order

        order_contact_data = (
            f"Айди клиента в вк: {order.customer_vk_id}, "
            f"Айди группы переписки клиента в вк: {order.customer_vk_messages_group_id}, "
            f"https://bluesales.ru/app/Customers/OrderView.aspx?id={order.id}"
        )

        if not (order.customer_vk_id and order.customer_vk_messages_group_id):
            logger.info(f"У клиента не указаны данные в вк для уведомления. {order_contact_data}")
            continue

        vk = VK_CLIENTS_BY_GROUP_ID[order.customer_vk_messages_group_id]
        result = vk.messages.send(
            user_id=order.customer_vk_id,
            message=text_for_postomat if is_postomat else text_for_pvz,
            random_id=int.from_bytes(os.getrandom(4), byteorder="big")
        )
        logger.debug("Результат отправки: " + str(result))
        logger.info(f"Отправка уведомления что заказ в {'постамат' if is_postomat else 'пункт выдачи'}. {order_contact_data}")


def get_crm_status_by_cdek(current_crm_status: str, cdek_status_name: str):
    return CDEK_TO_CRM_STATUS_ID.get(cdek_status_name, current_crm_status)

def main(*args, **kwargs):
    BLUESALES = BlueSales("managerYT10", "YT102025")
    CDEK = Client("XXsJ3TCwHbusuwgRNt6pgOeaq86Hj8o9", "lD1o1i6ZtyKiLxDyhuMHk52QxKoqwnxj")

    bluesales_orders = []

    for _ in range(3):
        try:
            bluesales_orders = BLUESALES.orders.get_all()
            break
        except BlueSalesError as e:
            print(e, "sleep 30 seconds...")
            sleep(30)

    print("Всего:", len(bluesales_orders), "сделок")

    # отсеиваем у кого нет статуса или номера трекера
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

    update_orders = []
    orders_notify_that_order_in_pvz = []  # заказы, заказчикам которых нужно сделать уведу что из заказ в ПВЗ


    for order in bluesales_orders:
        try:
            if order.status_name in ["Разбор", "Правки заказа"]:
                continue

            cdek_status = CDEK.get_order_info(order.tracking_number)["entity"]["statuses"][0]["code"]

            # logger.debug(str(order.id) + " " + cdek_status + " -> " + INVERTED_STATUSES[get_crm_status_by_cdek(order.status_name, cdek_status)])

            if (
                cdek_status != 'CREATED' and
                STATUSES[order.status_name] != get_crm_status_by_cdek(order.status_name, cdek_status)
            ):
                update_orders.append([order.id, get_crm_status_by_cdek(order.status_name, cdek_status), order.customer_id])

                logger.debug("New update: " + str(get_crm_status_by_cdek(order.status_name, cdek_status)) + f"а ожидает в пвз: {str(STATUSES['Ожидает в ПВЗ'])}")
                if get_crm_status_by_cdek(order.status_name, cdek_status) == STATUSES["Ожидает в ПВЗ"]:
                    is_postomat = cdek_status == "POSTOMAT_POSTED"
                    orders_notify_that_order_in_pvz.append((order, is_postomat))

        except HTTPError as e:
            logger.error(e)

    BLUESALES.orders.set_many_statuses(update_orders)

    notify_that_orders_in_pvz(orders_notify_that_order_in_pvz)

if __name__ == "__main__":
    logger.info(
        "=" * 10 + "  " + datetime.now().strftime("%d-%m-%Y %H:%M") + "  " + "=" * 10
    )
    try:
        main()
    except TimeoutError as e:
        logger.error(e)
    finally:
        logger.info("\n"*2)
