"""
Примеры использования клиента API Яндекс.Маркета
Демонстрирует основные сценарии работы с API
"""

from yandex_market_api import (
    YandexMarketClient, 
    AuthType, 
    OrderStatus, 
    ReportType,
    create_price_update_request,
    create_stock_update_request,
    wait_for_report
)
from config import get_config
from datetime import datetime, timedelta
import time


def example_basic_usage():
    """Базовый пример использования API"""
    
    # Загружаем конфигурацию из .env файла
    config = get_config()
    client = YandexMarketClient(config.api_key, AuthType.API_KEY)
    
    try:
        # 1. Получение списка кампаний
        print("=== Получение списка кампаний ===")
        campaigns_response = client.campaigns.get_campaigns()
        campaigns = campaigns_response.get('campaigns', [])
        
        if not campaigns:
            print("Кампании не найдены")
            return
        
        campaign_id = config.campaign_id
        print(f"Найдено кампаний: {len(campaigns)}")
        print(f"Используем кампанию: {campaign_id} - {campaign.get('domain', 'N/A')}")
        
        # 2. Получение заказов за последнюю неделю
        print("\n=== Получение заказов ===")
        from_date = datetime.now() - timedelta(days=7)
        orders_response = client.orders.get_orders(
            campaign_id=campaign_id,
            from_date=from_date,
            limit=10
        )
        
        orders = orders_response.get('orders', [])
        print(f"Найдено заказов за неделю: {len(orders)}")
        
        # Показываем информацию о первом заказе
        if orders:
            order = orders[0]
            print(f"Первый заказ: {order['id']}, статус: {order['status']}, сумма: {order['itemsTotal']}")
        
        # 3. Получение товаров
        print("\n=== Получение товаров ===")
        offers_response = client.offers.get_offers(campaign_id=campaign_id, limit=5)
        offers = offers_response.get('offers', [])
        print(f"Найдено товаров: {len(offers)}")
        
        if offers:
            offer = offers[0]
            print(f"Первый товар: {offer.get('id', 'N/A')}")
        
    except Exception as e:
        print(f"Ошибка: {e}")


def example_order_management():
    """Пример работы с заказами"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    campaign_id = config.campaign_id
    
    try:
        # Получение новых заказов
        print("=== Поиск новых заказов ===")
        new_orders = client.orders.get_orders(
            campaign_id=campaign_id,
            status=OrderStatus.PROCESSING,
            limit=50
        )
        
        for order in new_orders.get('orders', []):
            order_id = order['id']
            print(f"Обработка заказа {order_id}")
            
            # Получение детальной информации о заказе
            order_details = client.orders.get_order(campaign_id, order_id)
            
            # Получение информации о покупателе
            buyer_info = client.orders.get_order_buyer_info(campaign_id, order_id)
            print(f"Покупатель: {buyer_info.get('lastName', '')} {buyer_info.get('firstName', '')}")
            
            # Обновление статуса заказа (пример)
            # client.orders.update_order_status(campaign_id, order_id, OrderStatus.CONFIRMED)
            
    except Exception as e:
        print(f"Ошибка при работе с заказами: {e}")


def example_price_management():
    """Пример управления ценами товаров"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    campaign_id = config.campaign_id
    
    try:
        # Получение списка товаров
        print("=== Обновление цен товаров ===")
        offers_response = client.offers.get_offers(campaign_id=campaign_id, limit=10)
        offers = offers_response.get('offers', [])
        
        if not offers:
            print("Товары не найдены")
            return
        
        # Подготовка данных для обновления цен
        price_updates = []
        for offer in offers[:3]:  # Обновляем первые 3 товара
            offer_id = offer.get('id')
            if offer_id:
                # Увеличиваем цену на 10%
                current_price = offer.get('price', {}).get('value', 1000)
                new_price = current_price * 1.1
                
                price_update = create_price_update_request(offer_id, new_price)
                price_updates.append(price_update)
                print(f"Товар {offer_id}: {current_price} -> {new_price}")
        
        if price_updates:
            # Отправка обновления цен
            result = client.offers.update_offer_prices(campaign_id, price_updates)
            print(f"Результат обновления цен: {result}")
        
    except Exception as e:
        print(f"Ошибка при обновлении цен: {e}")


def example_stock_management():
    """Пример управления остатками товаров"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    campaign_id = config.campaign_id
    warehouse_id = 123  # ID склада
    
    try:
        print("=== Обновление остатков товаров ===")
        
        # Пример данных об остатках
        stock_updates = [
            create_stock_update_request("SKU-001", warehouse_id, 50),
            create_stock_update_request("SKU-002", warehouse_id, 25),
            create_stock_update_request("SKU-003", warehouse_id, 0),  # Товар закончился
        ]
        
        # Отправка обновления остатков
        result = client.offers.update_offer_stocks(campaign_id, stock_updates)
        print(f"Результат обновления остатков: {result}")
        
    except Exception as e:
        print(f"Ошибка при обновлении остатков: {e}")


def example_report_generation():
    """Пример генерации и получения отчетов"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    
    try:
        print("=== Генерация отчета по заказам ===")
        
        # Параметры отчета за последний месяц
        from_date = datetime.now() - timedelta(days=30)
        to_date = datetime.now()
        
        report_params = {
            "dateFrom": from_date.strftime("%Y-%m-%d"),
            "dateTo": to_date.strftime("%Y-%m-%d")
        }
        
        # Запуск генерации отчета
        report_response = client.reports.generate_report(
            ReportType.UNITED_ORDERS,
            report_params
        )
        
        report_id = report_response.get('reportId')
        if not report_id:
            print("Не удалось запустить генерацию отчета")
            return
        
        print(f"Запущена генерация отчета: {report_id}")
        
        # Ожидание готовности отчета
        print("Ожидание готовности отчета...")
        report_info = wait_for_report(client, report_id, max_wait_time=300)
        
        if report_info.get('status') == 'DONE':
            print("Отчет готов!")
            print(f"Ссылка на скачивание: {report_info.get('file')}")
            
            # Скачивание отчета
            report_data = client.reports.download_report(report_id)
            
            # Сохранение в файл
            filename = f"orders_report_{report_id}.xlsx"
            with open(filename, 'wb') as f:
                f.write(report_data)
            print(f"Отчет сохранен в файл: {filename}")
        
    except Exception as e:
        print(f"Ошибка при работе с отчетами: {e}")


def example_business_operations():
    """Пример работы с бизнес-операциями"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    business_id = config.business_id
    
    try:
        print("=== Работа с бизнес-настройками ===")
        
        # Получение настроек бизнеса
        business_settings = client.business.get_business_settings(business_id)
        print(f"Настройки бизнеса: {business_settings}")
        
        # Получение маппинга товаров
        print("\n=== Маппинг товаров ===")
        mappings = client.business.get_offer_mappings(business_id, limit=10)
        
        offers_mappings = mappings.get('offerMappings', [])
        print(f"Найдено маппингов: {len(offers_mappings)}")
        
        for mapping in offers_mappings[:3]:
            offer = mapping.get('offer', {})
            print(f"Товар: {offer.get('shopSku')} -> Маркет SKU: {mapping.get('mapping', {}).get('marketSku')}")
        
    except Exception as e:
        print(f"Ошибка при работе с бизнес-операциями: {e}")


def example_comprehensive_workflow():
    """Комплексный пример рабочего процесса"""
    
    config = get_config()
    client = YandexMarketClient(config.api_key)
    
    try:
        print("=== Комплексный рабочий процесс ===")
        
        # 1. Получение кампаний
        campaigns = client.campaigns.get_campaigns()
        if not campaigns.get('campaigns'):
            print("Кампании не найдены")
            return
        
        campaign_id = config.campaign_id
        print(f"Работаем с кампанией: {campaign_id}")
        
        # 2. Проверка новых заказов
        new_orders = client.orders.get_orders(
            campaign_id=campaign_id,
            status=OrderStatus.PROCESSING,
            limit=10
        )
        
        print(f"Новых заказов к обработке: {len(new_orders.get('orders', []))}")
        
        # 3. Проверка скрытых товаров
        hidden_offers = client.offers.get_hidden_offers(campaign_id, limit=5)
        print(f"Скрытых товаров: {len(hidden_offers.get('hiddenOffers', []))}")
        
        # 4. Поиск регионов (например, Москва)
        regions = client.search_regions("Москва", limit=5)
        print(f"Найдено регионов по запросу 'Москва': {len(regions.get('regions', []))}")
        
        # 5. Получение служб доставки
        delivery_services = client.get_delivery_services()
        print(f"Служб доставки: {len(delivery_services.get('deliveryServices', []))}")
        
        print("Рабочий процесс завершен успешно!")
        
    except Exception as e:
        print(f"Ошибка в рабочем процессе: {e}")


if __name__ == "__main__":
    print("Примеры использования API Яндекс.Маркета")
    print("Выберите пример для запуска:")
    print("1. Базовое использование")
    print("2. Управление заказами")
    print("3. Управление ценами")
    print("4. Управление остатками")
    print("5. Генерация отчетов")
    print("6. Бизнес-операции")
    print("7. Комплексный рабочий процесс")
    
    choice = input("Введите номер примера (1-7): ")
    
    examples = {
        "1": example_basic_usage,
        "2": example_order_management,
        "3": example_price_management,
        "4": example_stock_management,
        "5": example_report_generation,
        "6": example_business_operations,
        "7": example_comprehensive_workflow
    }
    
    if choice in examples:
        print(f"\nЗапуск примера {choice}...")
        examples[choice]()
    else:
        print("Неверный выбор!")
        example_basic_usage()  # Запускаем базовый пример по умолчанию 