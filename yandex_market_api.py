"""
Структурированный клиент для работы с API Яндекс.Маркета
Поддерживает все основные операции: заказы, товары, отчеты, склады
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Типы авторизации для API"""
    API_KEY = "api_key"
    OAUTH = "oauth"


class OrderStatus(Enum):
    """Статусы заказов"""
    CANCELLED = "CANCELLED"
    CONFIRMED = "CONFIRMED"
    DELIVERED = "DELIVERED"
    DELIVERY = "DELIVERY"
    PICKUP = "PICKUP"
    PROCESSING = "PROCESSING"
    UNPAID = "UNPAID"


class ReportType(Enum):
    """Типы отчетов"""
    UNITED_ORDERS = "united-orders"
    UNITED_RETURNS = "united-returns"
    GOODS_TURNOVER = "goods-turnover"
    GOODS_REALIZATION = "goods-realization"
    STOCKS_ON_WAREHOUSES = "stocks-on-warehouses"
    SHOWS_SALES = "shows-sales"
    PRICES = "prices"


@dataclass
class ApiConfig:
    """Конфигурация для API клиента"""
    base_url: str = "https://api.partner.market.yandex.ru"
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


class YandexMarketAPIError(Exception):
    """Базовое исключение для API Яндекс.Маркета"""
    pass


class YandexMarketAPI:
    """
    Основной класс для работы с API Яндекс.Маркета
    Поддерживает авторизацию через API-ключ или OAuth токен
    """
    
    def __init__(self, 
                 auth_token: str, 
                 auth_type: AuthType = AuthType.API_KEY,
                 config: Optional[ApiConfig] = None):
        """
        Инициализация API клиента
        
        Args:
            auth_token: API ключ или OAuth токен
            auth_type: Тип авторизации (API_KEY или OAUTH)
            config: Дополнительные настройки
        """
        self.auth_token = auth_token
        self.auth_type = auth_type
        self.config = config or ApiConfig()
        
        # Настройка заголовков для авторизации
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if auth_type == AuthType.API_KEY:
            self.headers["Api-Key"] = auth_token
        else:  # OAuth
            self.headers["Authorization"] = f"OAuth {auth_token}"
    
    def _make_request(self, 
                     method: str, 
                     endpoint: str, 
                     params: Optional[Dict] = None,
                     data: Optional[Dict] = None,
                     files: Optional[Dict] = None) -> Dict:
        """
        Выполнение HTTP запроса с обработкой ошибок и повторными попытками
        
        Args:
            method: HTTP метод (GET, POST, PUT, DELETE)
            endpoint: Эндпоинт API (без базового URL)
            params: Параметры запроса
            data: Данные для POST/PUT запросов
            files: Файлы для загрузки
            
        Returns:
            Ответ API в формате JSON
            
        Raises:
            YandexMarketAPIError: При ошибках API
        """
        url = f"{self.config.base_url}{endpoint}"
        
        # Подготовка данных запроса
        request_kwargs = {
            "headers": self.headers,
            "timeout": self.config.timeout,
            "params": params
        }
        
        if files:
            # Для загрузки файлов не используем JSON заголовок
            request_kwargs["headers"] = {k: v for k, v in self.headers.items() 
                                       if k != "Content-Type"}
            request_kwargs["files"] = files
            if data:
                request_kwargs["data"] = data
        elif data:
            request_kwargs["json"] = data
        
        # Выполнение запроса с повторными попытками
        for attempt in range(self.config.max_retries):
            try:
                logger.info(f"Выполнение {method} запроса к {endpoint}")
                response = requests.request(method, url, **request_kwargs)
                
                # Обработка ответа
                if response.status_code == 200:
                    return response.json() if response.content else {}
                elif response.status_code == 429:  # Rate limit
                    logger.warning(f"Превышен лимит запросов, ожидание...")
                    time.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                else:
                    error_msg = f"API ошибка {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    raise YandexMarketAPIError(error_msg)
                    
            except requests.RequestException as e:
                logger.error(f"Ошибка запроса: {e}")
                if attempt == self.config.max_retries - 1:
                    raise YandexMarketAPIError(f"Ошибка соединения: {e}")
                time.sleep(self.config.retry_delay)
        
        raise YandexMarketAPIError("Превышено количество попыток")


class CampaignManager:
    """Менеджер для работы с кампаниями (магазинами)"""
    
    def __init__(self, api: YandexMarketAPI):
        self.api = api
    
    def get_campaigns(self, page: int = 1, page_size: int = 50) -> Dict:
        """
        Получение списка кампаний (магазинов) пользователя
        
        Args:
            page: Номер страницы
            page_size: Размер страницы
            
        Returns:
            Список кампаний с пагинацией
        """
        params = {
            "page": page,
            "pageSize": page_size
        }
        return self.api._make_request("GET", "/campaigns", params=params)
    
    def get_campaign(self, campaign_id: int) -> Dict:
        """
        Получение информации о конкретной кампании
        
        Args:
            campaign_id: Идентификатор кампании
            
        Returns:
            Информация о кампании
        """
        return self.api._make_request("GET", f"/campaigns/{campaign_id}")
    
    def get_campaign_settings(self, campaign_id: int) -> Dict:
        """
        Получение настроек кампании
        
        Args:
            campaign_id: Идентификатор кампании
            
        Returns:
            Настройки кампании
        """
        return self.api._make_request("GET", f"/campaigns/{campaign_id}/settings")


class OrderManager:
    """Менеджер для работы с заказами"""
    
    def __init__(self, api: YandexMarketAPI):
        self.api = api
    
    def get_orders(self, 
                   campaign_id: int,
                   status: Optional[OrderStatus] = None,
                   from_date: Optional[datetime] = None,
                   to_date: Optional[datetime] = None,
                   page_token: Optional[str] = None,
                   limit: int = 50,
                   fake: bool = False) -> Dict:
        """
        Получение списка заказов с фильтрацией
        
        Args:
            campaign_id: Идентификатор кампании
            status: Статус заказов для фильтрации
            from_date: Дата начала периода
            to_date: Дата окончания периода
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            fake: Включать ли тестовые заказы
            
        Returns:
            Список заказов с пагинацией
        """
        params = {
            "limit": limit,
            "fake": fake
        }
        
        if status:
            params["status"] = status.value
        if from_date:
            params["fromDate"] = from_date.strftime("%d-%m-%Y")
        if to_date:
            params["toDate"] = to_date.strftime("%d-%m-%Y")
        if page_token:
            params["page_token"] = page_token
        
        return self.api._make_request("GET", f"/campaigns/{campaign_id}/orders", params=params)
    
    def get_order(self, campaign_id: int, order_id: int) -> Dict:
        """
        Получение детальной информации о заказе
        
        Args:
            campaign_id: Идентификатор кампании
            order_id: Идентификатор заказа
            
        Returns:
            Детальная информация о заказе
        """
        return self.api._make_request("GET", f"/campaigns/{campaign_id}/orders/{order_id}")
    
    def update_order_status(self, 
                           campaign_id: int, 
                           order_id: int, 
                           status: OrderStatus,
                           substatus: Optional[str] = None) -> Dict:
        """
        Обновление статуса заказа
        
        Args:
            campaign_id: Идентификатор кампании
            order_id: Идентификатор заказа
            status: Новый статус заказа
            substatus: Подстатус заказа
            
        Returns:
            Результат обновления
        """
        data = {"status": status.value}
        if substatus:
            data["substatus"] = substatus
        
        return self.api._make_request("PUT", f"/campaigns/{campaign_id}/orders/{order_id}/status", data=data)
    
    def accept_order_cancellation(self, campaign_id: int, order_id: int) -> Dict:
        """
        Принятие запроса на отмену заказа
        
        Args:
            campaign_id: Идентификатор кампании
            order_id: Идентификатор заказа
            
        Returns:
            Результат принятия отмены
        """
        return self.api._make_request("PUT", f"/campaigns/{campaign_id}/orders/{order_id}/cancellation/accept")
    
    def get_order_buyer_info(self, campaign_id: int, order_id: int) -> Dict:
        """
        Получение информации о покупателе заказа
        
        Args:
            campaign_id: Идентификатор кампании
            order_id: Идентификатор заказа
            
        Returns:
            Информация о покупателе
        """
        return self.api._make_request("GET", f"/campaigns/{campaign_id}/orders/{order_id}/buyer")


class OfferManager:
    """Менеджер для работы с товарами (офферами)"""
    
    def __init__(self, api: YandexMarketAPI):
        self.api = api
    
    def get_offers(self, campaign_id: int, page_token: Optional[str] = None, limit: int = 200) -> Dict:
        """
        Получение списка товаров через business API
        Сначала получает информацию о кампании для извлечения business_id
        
        Args:
            campaign_id: Идентификатор кампании
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            
        Returns:
            Список товаров с информацией о маппинге
        """
        # Получаем информацию о кампании для извлечения business_id
        campaign_response = self.api._make_request("GET", f"/campaigns/{campaign_id}")
        campaign_info = campaign_response.get('campaign', {})
        business_id = campaign_info.get('business', {}).get('id')
        
        if not business_id:
            raise YandexMarketAPIError("Не удалось получить business_id из кампании")
        
        # Используем business API для получения товаров
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        
        # POST запрос с пустым телом для получения всех товаров
        data = {}
        return self.api._make_request("POST", f"/businesses/{business_id}/offer-mappings", params=params, data=data)
    
    def get_campaign_offers(self, campaign_id: int, page_token: Optional[str] = None, limit: int = 200, 
                           offer_ids: Optional[List[str]] = None, statuses: Optional[List[str]] = None) -> Dict:
        """
        Получение детальной информации о товарах кампании (через POST запрос)
        
        Args:
            campaign_id: Идентификатор кампании
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            offer_ids: Список идентификаторов товаров для фильтрации
            statuses: Список статусов товаров для фильтрации
            
        Returns:
            Детальная информация о товарах
        """
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        
        # Подготовка тела запроса с фильтрами
        data = {}
        if offer_ids:
            data["offerIds"] = offer_ids
        if statuses:
            data["statuses"] = statuses
        
        return self.api._make_request("POST", f"/campaigns/{campaign_id}/offers", params=params, data=data)
    
    def update_offer_prices(self, campaign_id: int, offers: List[Dict]) -> Dict:
        """
        Обновление цен товаров
        
        Args:
            campaign_id: Идентификатор кампании
            offers: Список товаров с новыми ценами
                   [{"id": "offer_id", "price": {"value": 1000, "currencyId": "RUR"}}]
            
        Returns:
            Результат обновления цен
        """
        data = {"offers": offers}
        return self.api._make_request("POST", f"/campaigns/{campaign_id}/offer-prices/updates", data=data)
    
    def update_offer_stocks(self, campaign_id: int, skus: List[Dict]) -> Dict:
        """
        Обновление остатков товаров
        
        Args:
            campaign_id: Идентификатор кампании
            skus: Список SKU с остатками
                  [{"sku": "sku_id", "warehouseId": 123, "items": [{"count": 10, "type": "FIT"}]}]
            
        Returns:
            Результат обновления остатков
        """
        data = {"skus": skus}
        return self.api._make_request("PUT", f"/campaigns/{campaign_id}/offers/stocks", data=data)
    
    def get_hidden_offers(self, campaign_id: int, page_token: Optional[str] = None, limit: int = 200) -> Dict:
        """
        Получение списка скрытых товаров
        
        Args:
            campaign_id: Идентификатор кампании
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            
        Returns:
            Список скрытых товаров
        """
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        
        return self.api._make_request("GET", f"/campaigns/{campaign_id}/hidden-offers", params=params)
    
    def add_hidden_offers(self, campaign_id: int, hidden_offers: List[Dict]) -> Dict:
        """
        Добавление товаров в скрытые
        
        Args:
            campaign_id: Идентификатор кампании
            hidden_offers: Список товаров для скрытия
                          [{"offerId": "offer_id", "comment": "Причина скрытия"}]
            
        Returns:
            Результат скрытия товаров
        """
        data = {"hiddenOffers": hidden_offers}
        return self.api._make_request("POST", f"/campaigns/{campaign_id}/hidden-offers", data=data)
    
    def get_warehouse_stocks(self, campaign_id: int, 
                           warehouse_id: Optional[int] = None,
                           offer_ids: Optional[List[str]] = None,
                           page_token: Optional[str] = None,
                           limit: int = 200) -> Dict:
        """
        Получение остатков товаров на складах
        
        Args:
            campaign_id: Идентификатор кампании
            warehouse_id: Идентификатор склада (опционально)
            offer_ids: Список SKU товаров (опционально)
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            
        Returns:
            Информация об остатках товаров
        """
        data = {}
        
        if warehouse_id:
            data["stocksWarehouseId"] = warehouse_id
        if offer_ids:
            data["offerIds"] = offer_ids
        if page_token:
            data["page_token"] = page_token
        if not offer_ids:  # Пагинация работает только без конкретных SKU
            data["limit"] = limit
            
        return self.api._make_request("POST", f"/campaigns/{campaign_id}/offers/stocks", data=data)


class ReportManager:
    """Менеджер для работы с отчетами"""
    
    def __init__(self, api: YandexMarketAPI):
        self.api = api
    
    def generate_report(self, 
                       report_type: ReportType,
                       params: Optional[Dict] = None) -> Dict:
        """
        Генерация отчета
        
        Args:
            report_type: Тип отчета
            params: Параметры отчета (даты, фильтры и т.д.)
            
        Returns:
            Информация о запущенной генерации отчета
        """
        endpoint = f"/reports/{report_type.value}/generate"
        return self.api._make_request("POST", endpoint, data=params or {})
    
    def get_report_info(self, report_id: str) -> Dict:
        """
        Получение информации о статусе генерации отчета
        
        Args:
            report_id: Идентификатор отчета
            
        Returns:
            Информация о статусе отчета
        """
        return self.api._make_request("GET", f"/reports/info/{report_id}")
    
    def download_report(self, report_id: str) -> bytes:
        """
        Скачивание готового отчета
        
        Args:
            report_id: Идентификатор отчета
            
        Returns:
            Содержимое файла отчета
        """
        # Для скачивания файлов используем отдельный запрос
        url = f"{self.api.config.base_url}/reports/info/{report_id}"
        response = requests.get(url, headers=self.api.headers)
        
        if response.status_code == 200:
            report_info = response.json()
            if report_info.get("status") == "DONE" and report_info.get("file"):
                # Скачиваем файл отчета
                file_response = requests.get(report_info["file"], headers=self.api.headers)
                return file_response.content
        
        raise YandexMarketAPIError("Отчет не готов или произошла ошибка")


class BusinessManager:
    """Менеджер для работы с бизнесом"""
    
    def __init__(self, api: YandexMarketAPI):
        self.api = api
    
    def get_business_settings(self, business_id: int) -> Dict:
        """
        Получение настроек бизнеса
        
        Args:
            business_id: Идентификатор бизнеса
            
        Returns:
            Настройки бизнеса
        """
        return self.api._make_request("GET", f"/businesses/{business_id}/settings")
    
    def get_offer_mappings(self, 
                          business_id: int,
                          page_token: Optional[str] = None,
                          limit: int = 200) -> Dict:
        """
        Получение информации о маппинге товаров
        
        Args:
            business_id: Идентификатор бизнеса
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            
        Returns:
            Информация о маппинге товаров
        """
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        
        return self.api._make_request("GET", f"/businesses/{business_id}/offer-mappings", params=params)
    
    def update_offer_mappings(self, business_id: int, offers: List[Dict]) -> Dict:
        """
        Обновление маппинга товаров
        
        Args:
            business_id: Идентификатор бизнеса
            offers: Список товаров для обновления маппинга
            
        Returns:
            Результат обновления маппинга
        """
        data = {"offers": offers}
        return self.api._make_request("POST", f"/businesses/{business_id}/offer-mappings/update", data=data)


class YandexMarketClient:
    """
    Главный клиент для работы с API Яндекс.Маркета
    Объединяет все менеджеры и предоставляет удобный интерфейс
    """
    
    def __init__(self, 
                 auth_token: str, 
                 auth_type: AuthType = AuthType.API_KEY,
                 config: Optional[ApiConfig] = None):
        """
        Инициализация клиента
        
        Args:
            auth_token: API ключ или OAuth токен
            auth_type: Тип авторизации
            config: Дополнительные настройки
        """
        self.api = YandexMarketAPI(auth_token, auth_type, config)
        
        # Инициализация менеджеров
        self.campaigns = CampaignManager(self.api)
        self.orders = OrderManager(self.api)
        self.offers = OfferManager(self.api)
        self.reports = ReportManager(self.api)
        self.business = BusinessManager(self.api)
    
    def search_regions(self, name: str, page_token: Optional[str] = None, limit: int = 10) -> Dict:
        """
        Поиск регионов по имени
        
        Args:
            name: Имя региона для поиска
            page_token: Токен для пагинации
            limit: Лимит записей на странице
            
        Returns:
            Список найденных регионов
        """
        params = {"name": name, "limit": limit}
        if page_token:
            params["page_token"] = page_token
        return self.api._make_request("GET", "/regions", params=params)
    
    def get_delivery_services(self) -> Dict:
        """
        Получение списка служб доставки
        
        Returns:
            Список служб доставки
        """
        return self.api._make_request("GET", "/delivery/services")
    
    def calculate_tariffs(self, 
                         campaign_id: int,
                         calculate_request: Dict) -> Dict:
        """
        Расчет тарифов доставки
        
        Args:
            campaign_id: Идентификатор кампании
            calculate_request: Параметры для расчета тарифов
            
        Returns:
            Рассчитанные тарифы
        """
        return self.api._make_request("POST", "/tariffs/calculate", data=calculate_request)


# Примеры использования и вспомогательные функции

def create_price_update_request(offer_id: str, price: float, currency: str = "RUR") -> Dict:
    """
    Создание запроса для обновления цены товара
    
    Args:
        offer_id: Идентификатор товара
        price: Новая цена
        currency: Валюта
        
    Returns:
        Структура запроса для обновления цены
    """
    return {
        "id": offer_id,
        "price": {
            "value": price,
            "currencyId": currency
        }
    }


def create_stock_update_request(sku: str, warehouse_id: int, count: int, stock_type: str = "FIT") -> Dict:
    """
    Создание запроса для обновления остатков
    
    Args:
        sku: SKU товара
        warehouse_id: Идентификатор склада
        count: Количество
        stock_type: Тип остатка (FIT, DEFECT)
        
    Returns:
        Структура запроса для обновления остатков
    """
    return {
        "sku": sku,
        "warehouseId": warehouse_id,
        "items": [
            {
                "count": count,
                "type": stock_type
            }
        ]
    }


def wait_for_report(client: YandexMarketClient, report_id: str, max_wait_time: int = 600) -> Dict:
    """
    Ожидание готовности отчета с периодической проверкой статуса
    
    Args:
        client: Клиент API
        report_id: Идентификатор отчета
        max_wait_time: Максимальное время ожидания в секундах
        
    Returns:
        Информация о готовом отчете
        
    Raises:
        YandexMarketAPIError: Если отчет не готов в течение указанного времени
    """
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        report_info = client.reports.get_report_info(report_id)
        
        status = report_info.get("status")
        if status == "DONE":
            return report_info
        elif status == "ERROR":
            raise YandexMarketAPIError(f"Ошибка генерации отчета: {report_info.get('error', 'Неизвестная ошибка')}")
        
        logger.info(f"Отчет {report_id} в статусе {status}, ожидание...")
        time.sleep(30)  # Проверяем каждые 30 секунд
    
    raise YandexMarketAPIError(f"Превышено время ожидания готовности отчета {report_id}")


# Пример использования
if __name__ == "__main__":
    # Загружаем конфигурацию
    from config import get_config
    config = get_config()
    
    # Инициализация клиента с API ключом
    client = YandexMarketClient(config.api_key, AuthType.API_KEY)
    
    try:
        # Получение списка кампаний
        campaigns = client.campaigns.get_campaigns()
        print(f"Найдено кампаний: {len(campaigns.get('campaigns', []))}")
        
        if campaigns.get('campaigns'):
            campaign_id = config.campaign_id
            
            # Получение заказов за последние 7 дней
            from_date = datetime.now() - timedelta(days=7)
            orders = client.orders.get_orders(
                campaign_id=campaign_id,
                from_date=from_date,
                limit=10
            )
            print(f"Найдено заказов: {len(orders.get('orders', []))}")
            
            # Получение товаров
            offers = client.offers.get_offers(campaign_id=campaign_id, limit=10)
            print(f"Найдено товаров: {len(offers.get('offers', []))}")
            
            # Генерация отчета по заказам
            report_params = {
                "dateFrom": from_date.strftime("%Y-%m-%d"),
                "dateTo": datetime.now().strftime("%Y-%m-%d"),
                "businessId": config.business_id
            }
            report_response = client.reports.generate_report(
                ReportType.UNITED_ORDERS, 
                report_params
            )
            print(f"Запущена генерация отчета: {report_response.get('reportId')}")
            
    except YandexMarketAPIError as e:
        logger.error(f"Ошибка API: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")