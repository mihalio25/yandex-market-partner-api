#!/usr/bin/env python3
"""
Скрипт для обновления цен товаров из CSV файла
"""

import argparse
import csv
import sys
import time
from datetime import datetime
from typing import List, Dict, Optional
import os

from yandex_market_api import YandexMarketClient, AuthType
from config import get_config


class CSVPriceUpdater:
    def __init__(self, api_key: str, campaign_id: int, dry_run: bool = True):
        self.client = YandexMarketClient(api_key, AuthType.API_KEY)
        self.campaign_id = campaign_id
        self.dry_run = dry_run
        self.log_file = f"csv_price_updates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.log_entries = []
        self._get_campaign_info()
    
    def log(self, message: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        self.log_entries.append(log_entry)
    
    def _get_campaign_info(self):
        try:
            campaign_response = self.client.campaigns.get_campaign(self.campaign_id)
            self.campaign_info = campaign_response.get('campaign', {})
            self.business_id = self.campaign_info.get('business', {}).get('id')
            
            if not self.business_id:
                raise ValueError("Не удалось получить business_id")
            
            self.log(f"Кампания: {self.campaign_info.get('domain', 'N/A')} (ID: {self.campaign_id})")
            self.log(f"Бизнес ID: {self.business_id}")
            
        except Exception as e:
            self.log(f"Ошибка получения информации о кампании: {e}")
            raise
    
    def read_csv_prices(self, csv_file: str, sku_column: str = 'sku', price_column: str = 'old_price') -> Dict[str, float]:
        prices_dict = {}
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                if sku_column not in reader.fieldnames:
                    raise ValueError(f"Колонка '{sku_column}' не найдена")
                if price_column not in reader.fieldnames:
                    raise ValueError(f"Колонка '{price_column}' не найдена")
                
                for row_num, row in enumerate(reader, start=2):
                    sku = row[sku_column].strip()
                    price_str = row[price_column].strip()
                    
                    if not sku or not price_str:
                        continue
                    
                    try:
                        price = float(price_str)
                        if price > 0:
                            prices_dict[sku] = price
                    except ValueError:
                        self.log(f"Строка {row_num}: неверная цена '{price_str}' для SKU {sku}")
                        continue
            
            self.log(f"Загружено цен из CSV: {len(prices_dict)}")
            return prices_dict
            
        except Exception as e:
            self.log(f"Ошибка чтения CSV: {e}")
            return {}
    
    def get_current_offers(self, max_offers: int = 1000) -> Dict[str, Dict]:
        try:
            self.log("Получение товаров...")
            
            offers_dict = {}
            page_token = None
            page_size = 100  # Максимальный размер страницы
            total_received = 0
            
            while total_received < max_offers:
                params = {"limit": min(page_size, max_offers - total_received)}
                data = {}
                
                if page_token:
                    params["page_token"] = page_token
                
                offers_response = self.client.api._make_request(
                    "POST", 
                    f"/businesses/{self.business_id}/offer-mappings", 
                    params=params, 
                    data=data
                )
                
                result = offers_response.get('result', {})
                offer_mappings = result.get('offerMappings', [])
                
                if not offer_mappings:
                    break
                
                self.log(f"Получено товаров в текущей странице: {len(offer_mappings)}")
                
                for mapping in offer_mappings:
                    offer = mapping.get('offer', {})
                    sku = offer.get('offerId')
                    
                    if sku and 'basicPrice' in offer:
                        price_info = offer['basicPrice']
                        if isinstance(price_info, dict) and 'value' in price_info:
                            offers_dict[sku] = {
                                'name': offer.get('name', 'Без названия'),
                                'current_price': float(price_info['value']),
                                'currency': price_info.get('currencyId', 'RUR')
                            }
                
                total_received += len(offer_mappings)
                
                # Получаем токен следующей страницы
                paging = result.get('paging', {})
                next_page_token = paging.get('nextPageToken')
                
                if not next_page_token:
                    break
                    
                page_token = next_page_token
                time.sleep(0.5)  # Небольшая задержка между запросами
            
            self.log(f"Всего получено товаров с ценами: {len(offers_dict)}")
            return offers_dict
            
        except Exception as e:
            self.log(f"Ошибка получения товаров: {e}")
            return {}
    
    def update_prices_from_csv(self, csv_file: str, sku_column: str = 'sku', price_column: str = 'old_price', batch_size: int = 50) -> Dict:
        self.log(f"Обновление цен из CSV: {csv_file}")
        
        csv_prices = self.read_csv_prices(csv_file, sku_column, price_column)
        if not csv_prices:
            return {'success': 0, 'errors': 0, 'skipped': 0, 'not_found': 0}
        
        current_offers = self.get_current_offers()
        if not current_offers:
            return {'success': 0, 'errors': 0, 'skipped': 0, 'not_found': len(csv_prices)}
        
        price_updates = []
        stats = {'success': 0, 'errors': 0, 'skipped': 0, 'not_found': 0}
        
        for sku, new_price in csv_prices.items():
            if sku not in current_offers:
                self.log(f"SKU {sku} не найден")
                stats['not_found'] += 1
                continue
            
            offer = current_offers[sku]
            current_price = offer['current_price']
            
            if abs(new_price - current_price) < 0.01:
                stats['skipped'] += 1
                continue
            
            price_update = {
                "offerId": sku,
                "price": {
                    "value": round(new_price, 2),
                    "currencyId": offer['currency']
                }
            }
            price_updates.append(price_update)
            
            self.log(f"SKU {sku}: {current_price:.2f} → {new_price:.2f}")
        
        if not price_updates:
            return stats
        
        if self.dry_run:
            self.log(f"ТЕСТОВЫЙ РЕЖИМ: Запланировано {len(price_updates)} обновлений")
            stats['success'] = len(price_updates)
        else:
            self.log(f"Обновление {len(price_updates)} цен...")
            
            for i in range(0, len(price_updates), batch_size):
                batch = price_updates[i:i + batch_size]
                
                try:
                    result = self.client.api._make_request(
                        "POST", 
                        f"/businesses/{self.business_id}/offer-prices/updates",
                        data={"offers": batch}
                    )
                    
                    if result.get('status') == 'OK':
                        stats['success'] += len(batch)
                        self.log(f"Обновлено цен: {len(batch)}")
                    else:
                        stats['errors'] += len(batch)
                        self.log(f"Ошибка: {result}")
                        
                except Exception as e:
                    stats['errors'] += len(batch)
                    self.log(f"Ошибка: {e}")
                
                time.sleep(1)
        
        return stats


def main():
    parser = argparse.ArgumentParser(description='Обновление цен из CSV файла')
    parser.add_argument('--csv-file', required=True, help='Путь к CSV файлу')
    parser.add_argument('--sku-column', default='sku', help='Колонка SKU')
    parser.add_argument('--price-column', default='old_price', help='Колонка цены')
    parser.add_argument('--api-key', help='API ключ')
    parser.add_argument('--campaign-id', type=int, help='ID кампании')
    parser.add_argument('--dry-run', action='store_true', help='Тестовый режим')
    parser.add_argument('--batch-size', type=int, default=50, help='Размер пакета')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Файл не найден: {args.csv_file}")
        return 1
    
    config = get_config()
    api_key = args.api_key or config.api_key
    campaign_id = args.campaign_id or config.campaign_id
    
    if not api_key:
        print("API ключ не указан")
        return 1
    
    try:
        updater = CSVPriceUpdater(api_key, campaign_id, args.dry_run)
        stats = updater.update_prices_from_csv(
            args.csv_file,
            args.sku_column,
            args.price_column,
            args.batch_size
        )
        
        print(f"\nРезультат:")
        print(f"Успешно: {stats['success']}")
        print(f"Ошибок: {stats['errors']}")
        print(f"Пропущено: {stats['skipped']}")
        print(f"Не найдено: {stats['not_found']}")
        
        return 0 if stats['errors'] == 0 else 1
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 