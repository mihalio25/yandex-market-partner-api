#!/usr/bin/env python3
"""
Скрипт для получения всех товаров с их ценами через Yandex Market Partner API
и сохранения данных в CSV файл.

Использование:
    python get_products_with_prices.py [--output filename.csv] [--campaign-id ID]
"""

import csv
import json
import time
import argparse
from datetime import datetime
from typing import List, Dict, Set, Optional
from dataclasses import dataclass

from yandex_market_api import YandexMarketClient, AuthType, YandexMarketAPIError
from config import get_config

@dataclass
class ProductData:
    """Структура данных о товаре"""
    offer_id: str
    name: str
    vendor: str
    price: Optional[float]
    currency: str
    price_vat: Optional[str]
    updated_at: Optional[str]
    status: str
    market_category_id: Optional[int]
    errors: List[str]
    warnings: List[str]


class ProductPriceCollector:
    """Класс для сбора данных о товарах и их ценах"""
    
    def __init__(self, api_key: str, campaign_id: int):
        """
        Инициализация коллектора
        
        Args:
            api_key: Ключ API Яндекс.Маркета
            campaign_id: ID кампании (магазина)
        """
        self.client = YandexMarketClient(api_key, AuthType.API_KEY)
        self.campaign_id = campaign_id
        self.products: List[ProductData] = []
        
    def get_all_offers(self) -> List[Dict]:
        """
        Получение всех товаров из кампании с пагинацией
        
        Returns:
            Список всех товаров
        """
        print("🔍 Получение списка товаров...")
        
        all_offers = []
        page_token = None
        page_num = 1
        
        while True:
            try:
                print(f"📄 Загрузка страницы {page_num}...")
                
                # Получаем товары текущей страницы
                response = self.client.offers.get_offers(
                    campaign_id=self.campaign_id,
                    page_token=page_token,
                    limit=200  # Максимальный размер страницы
                )
                
                offers = response.get('offers', [])
                if not offers:
                    print("✅ Больше товаров не найдено")
                    break
                
                all_offers.extend(offers)
                print(f"📦 Найдено {len(offers)} товаров на странице {page_num}")
                
                # Проверяем наличие следующей страницы
                paging = response.get('paging', {})
                page_token = paging.get('nextPageToken')
                
                if not page_token:
                    print("✅ Все страницы обработаны")
                    break
                
                page_num += 1
                
                # Небольшая задержка между запросами
                time.sleep(0.1)
                
            except YandexMarketAPIError as e:
                print(f"❌ Ошибка при получении товаров: {e}")
                break
        
        print(f"📊 Всего найдено товаров: {len(all_offers)}")
        return all_offers
    
    def get_prices_for_offers(self, offer_ids: List[str]) -> Dict[str, Dict]:
        """
        Получение цен для списка товаров
        
        Args:
            offer_ids: Список идентификаторов товаров
            
        Returns:
            Словарь с ценами для каждого товара
        """
        if not offer_ids:
            return {}
        
        try:
            print(f"💰 Получение цен для {len(offer_ids)} товаров...")
            
            # Разбиваем на части по 1000 товаров (лимит API)
            batch_size = 1000
            all_prices = {}
            
            for i in range(0, len(offer_ids), batch_size):
                batch = offer_ids[i:i + batch_size]
                print(f"💸 Обработка пакета {i//batch_size + 1}: товары {i+1}-{min(i+batch_size, len(offer_ids))}")
                
                # Запрос цен для текущего пакета
                request_body = {
                    "offerIds": batch
                }
                
                response = self.client.api._make_request(
                    "POST", 
                    f"/campaigns/{self.campaign_id}/offer-prices",
                    data=request_body
                )
                
                # Обрабатываем ответ
                offers_data = response.get('offers', [])
                for offer_data in offers_data:
                    offer_id = offer_data.get('offerId')
                    if offer_id:
                        all_prices[offer_id] = offer_data
                
                # Задержка между пакетами
                if i + batch_size < len(offer_ids):
                    time.sleep(0.2)
            
            print(f"✅ Получено цен для {len(all_prices)} товаров")
            return all_prices
            
        except YandexMarketAPIError as e:
            print(f"❌ Ошибка при получении цен: {e}")
            return {}
    
    def process_offers_data(self, offers: List[Dict], prices_data: Dict[str, Dict]) -> None:
        """
        Обработка данных о товарах и ценах
        
        Args:
            offers: Список товаров
            prices_data: Данные о ценах
        """
        print("🔄 Обработка данных о товарах...")
        
        for offer in offers:
            try:
                offer_id = offer.get('offerId', '')
                
                # Базовая информация о товаре
                name = offer.get('name', '')
                vendor = offer.get('vendor', '')
                status = offer.get('status', 'UNKNOWN')
                market_category_id = offer.get('marketCategoryId')
                
                # Ошибки и предупреждения
                errors = [error.get('message', '') for error in offer.get('errors', [])]
                warnings = [warning.get('message', '') for warning in offer.get('warnings', [])]
                
                # Цена из основных данных товара
                price = None
                currency = 'RUR'
                price_vat = None
                updated_at = None
                
                # Проверяем цену в основных данных
                basic_price = offer.get('basicPrice', {})
                campaign_price = offer.get('campaignPrice', {})
                
                if campaign_price.get('value'):
                    price = float(campaign_price['value'])
                    currency = campaign_price.get('currency', 'RUR')
                    price_vat = campaign_price.get('vat')
                elif basic_price.get('value'):
                    price = float(basic_price['value'])
                    currency = basic_price.get('currency', 'RUR')
                
                # Проверяем цену в отдельном запросе цен
                if offer_id in prices_data:
                    price_info = prices_data[offer_id]
                    price_data = price_info.get('price', {})
                    if price_data.get('value'):
                        price = float(price_data['value'])
                        currency = price_data.get('currency', 'RUR')
                        updated_at = price_info.get('updatedAt')
                
                # Создаем объект данных о товаре
                product = ProductData(
                    offer_id=offer_id,
                    name=name,
                    vendor=vendor,
                    price=price,
                    currency=currency,
                    price_vat=price_vat,
                    updated_at=updated_at,
                    status=status,
                    market_category_id=market_category_id,
                    errors=errors,
                    warnings=warnings
                )
                
                self.products.append(product)
                
            except Exception as e:
                print(f"⚠️ Ошибка при обработке товара {offer.get('offerId', 'UNKNOWN')}: {e}")
        
        print(f"✅ Обработано {len(self.products)} товаров")
    
    def collect_all_data(self) -> List[ProductData]:
        """
        Сбор всех данных о товарах и ценах
        
        Returns:
            Список данных о товарах
        """
        print(f"🚀 Начинаем сбор данных для кампании {self.campaign_id}")
        
        # 1. Получаем все товары
        offers = self.get_all_offers()
        if not offers:
            print("❌ Товары не найдены")
            return []
        
        # 2. Извлекаем ID товаров для запроса цен
        offer_ids = [offer.get('offerId') for offer in offers if offer.get('offerId')]
        print(f"🔢 Уникальных ID товаров: {len(set(offer_ids))}")
        
        # 3. Получаем цены для товаров
        prices_data = self.get_prices_for_offers(list(set(offer_ids)))
        
        # 4. Обрабатываем все данные
        self.process_offers_data(offers, prices_data)
        
        return self.products
    
    def save_to_csv(self, filename: str) -> None:
        """
        Сохранение данных в CSV файл
        
        Args:
            filename: Имя файла для сохранения
        """
        if not self.products:
            print("❌ Нет данных для сохранения")
            return
        
        print(f"💾 Сохранение данных в файл {filename}...")
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'offer_id',
                    'name', 
                    'vendor',
                    'price',
                    'currency',
                    'price_vat',
                    'updated_at',
                    'status',
                    'market_category_id',
                    'errors_count',
                    'warnings_count',
                    'errors',
                    'warnings'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for product in self.products:
                    writer.writerow({
                        'offer_id': product.offer_id,
                        'name': product.name,
                        'vendor': product.vendor,
                        'price': product.price if product.price is not None else '',
                        'currency': product.currency,
                        'price_vat': product.price_vat or '',
                        'updated_at': product.updated_at or '',
                        'status': product.status,
                        'market_category_id': product.market_category_id or '',
                        'errors_count': len(product.errors),
                        'warnings_count': len(product.warnings),
                        'errors': '; '.join(product.errors) if product.errors else '',
                        'warnings': '; '.join(product.warnings) if product.warnings else ''
                    })
            
            print(f"✅ Данные успешно сохранены в файл {filename}")
            print(f"📊 Сохранено записей: {len(self.products)}")
            
            # Статистика
            products_with_price = sum(1 for p in self.products if p.price is not None)
            products_with_errors = sum(1 for p in self.products if p.errors)
            
            print(f"📈 Товаров с ценами: {products_with_price}")
            print(f"⚠️ Товаров с ошибками: {products_with_errors}")
            
        except Exception as e:
            print(f"❌ Ошибка при сохранении файла: {e}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='Получение товаров с ценами из Yandex Market API'
    )
    parser.add_argument(
        '--output', '-o',
        default=f'products_with_prices_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
        help='Имя выходного CSV файла'
    )
    parser.add_argument(
        '--campaign-id', '-c',
        type=int,
        help='ID кампании (по умолчанию из конфигурации)'
    )
    
    args = parser.parse_args()
    
    try:
        # Загрузка конфигурации
        config = get_config()
        campaign_id = args.campaign_id or config.campaign_id
        
        print("=" * 60)
        print("🛍️ YANDEX MARKET PRODUCTS & PRICES COLLECTOR")
        print("=" * 60)
        print(f"🏪 Кампания ID: {campaign_id}")
        print(f"📄 Выходной файл: {args.output}")
        print(f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Создание коллектора и сбор данных
        collector = ProductPriceCollector(config.api_key, campaign_id)
        products = collector.collect_all_data()
        
        if products:
            collector.save_to_csv(args.output)
            print("🎉 Задача выполнена успешно!")
        else:
            print("😞 Данные не найдены")
        
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        print("💡 Проверьте файл config.env или переменные окружения")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 