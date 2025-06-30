#!/usr/bin/env python3
"""
Исправленный скрипт для экспорта товаров с ценами
Использует правильные API endpoints на основе price_updater.py
"""

import csv
import time
from datetime import datetime
from typing import List, Dict
from yandex_market_api import YandexMarketClient, AuthType, YandexMarketAPIError
from config import get_config

def get_business_id(client: YandexMarketClient, campaign_id: int) -> int:
    """
    Получение business_id из информации о кампании
    """
    try:
        campaign_response = client.campaigns.get_campaign(campaign_id)
        campaign_info = campaign_response.get('campaign', {})
        business_id = campaign_info.get('business', {}).get('id')
        
        if not business_id:
            raise ValueError("Не удалось получить business_id из кампании")
        
        print(f"✅ Получен business_id: {business_id}")
        print(f"📊 Кампания: {campaign_info.get('domain', 'N/A')}")
        
        return business_id
        
    except Exception as e:
        print(f"❌ Ошибка получения business_id: {e}")
        raise

def get_all_offers_with_prices(client: YandexMarketClient, business_id: int) -> List[Dict]:
    """
    Получение всех товаров с ценами через business API
    """
    print("🔍 Получение товаров через business API...")
    
    all_offers = []
    page_token = None
    page_num = 1
    
    while True:
        try:
            print(f"📄 Загрузка страницы {page_num}...")
            
            # Параметры запроса
            params = {"limit": 200}
            if page_token:
                params["pageToken"] = page_token
            
            # POST запрос с пустым телом для получения всех товаров
            response = client.api._make_request(
                "POST", 
                f"/businesses/{business_id}/offer-mappings",
                params=params,
                data={}
            )
            
            # Извлекаем товары из ответа
            result = response.get('result', {})
            offer_mappings = result.get('offerMappings', [])
            
            if not offer_mappings:
                print("✅ Больше товаров не найдено")
                break
            
            print(f"📦 Найдено {len(offer_mappings)} товаров на странице {page_num}")
            
            # Обрабатываем товары
            for mapping in offer_mappings:
                offer = mapping.get('offer', {})
                mapping_data = mapping.get('mapping', {})
                
                # Извлекаем информацию о товаре
                offer_info = {
                    'offer_id': offer.get('offerId', ''),
                    'name': offer.get('name', ''),
                    'vendor': offer.get('vendor', ''),
                    'category': mapping_data.get('marketCategoryName', ''),
                    'market_category_id': mapping_data.get('marketCategoryId', ''),
                    'price': None,
                    'currency': 'RUR',
                    'availability': offer.get('availability', ''),
                    'barcode': offer.get('barcodes', []),
                    'vendor_code': offer.get('vendorCode', ''),
                    'description': offer.get('description', ''),
                    'pictures': offer.get('pictures', []),
                    'market_sku': mapping_data.get('marketSku', ''),
                }
                
                # Извлекаем цену
                basic_price = offer.get('basicPrice')
                if basic_price and isinstance(basic_price, dict):
                    price_value = basic_price.get('value')
                    if price_value:
                        offer_info['price'] = float(price_value)
                        offer_info['currency'] = basic_price.get('currencyId', 'RUR')
                
                all_offers.append(offer_info)
            
            # Проверяем пагинацию
            paging = result.get('paging', {})
            page_token = paging.get('nextPageToken')
            
            if not page_token:
                print("✅ Все страницы обработаны")
                break
            
            page_num += 1
            time.sleep(0.1)  # Небольшая пауза между запросами
            
        except YandexMarketAPIError as e:
            print(f"❌ Ошибка при получении товаров: {e}")
            break
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            break
    
    print(f"📊 Всего найдено товаров: {len(all_offers)}")
    return all_offers

def save_to_csv(offers: List[Dict], filename: str):
    """
    Сохранение товаров в CSV файл
    """
    if not offers:
        print("❌ Нет данных для сохранения")
        return
    
    print(f"💾 Сохранение {len(offers)} товаров в {filename}...")
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'offer_id',
                'name',
                'vendor',
                'category',
                'market_category_id',
                'price',
                'currency',
                'availability',
                'market_sku',
                'vendor_code',
                'description'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for offer in offers:
                writer.writerow({
                    'offer_id': offer.get('offer_id', ''),
                    'name': offer.get('name', ''),
                    'vendor': offer.get('vendor', ''),
                    'category': offer.get('category', ''),
                    'market_category_id': offer.get('market_category_id', ''),
                    'price': offer.get('price', ''),
                    'currency': offer.get('currency', ''),
                    'availability': offer.get('availability', ''),
                    'market_sku': offer.get('market_sku', ''),
                    'vendor_code': offer.get('vendor_code', ''),
                    'description': offer.get('description', '')[:100] + '...' if len(offer.get('description', '')) > 100 else offer.get('description', ''),
                })
        
        print(f"✅ Данные сохранены в {filename}")
        
        # Статистика
        with_price = sum(1 for offer in offers if offer.get('price'))
        with_mapping = sum(1 for offer in offers if offer.get('market_sku'))
        available = sum(1 for offer in offers if offer.get('availability') == 'ACTIVE')
        
        print(f"\n📊 Статистика:")
        print(f"   - Всего товаров: {len(offers)}")
        print(f"   - С ценами: {with_price}")
        print(f"   - С привязкой к Маркету: {with_mapping}")
        print(f"   - Активных: {available}")
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")

def main():
    """Основная функция"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'products_fixed_{timestamp}.csv'
    
    print("=" * 60)
    print("🔧 ИСПРАВЛЕННЫЙ ЭКСПОРТ ТОВАРОВ С ЦЕНАМИ")
    print("=" * 60)
    
    try:
        # Загружаем конфигурацию
        config = get_config()
        client = YandexMarketClient(config.api_key, AuthType.API_KEY)
        
        print(f"🏪 Кампания: {config.campaign_id}")
        print(f"📄 Файл: {output_file}")
        print()
        
        # Получаем business_id
        business_id = get_business_id(client, config.campaign_id)
        
        # Получаем все товары
        offers = get_all_offers_with_prices(client, business_id)
        
        if offers:
            # Сохраняем в CSV
            save_to_csv(offers, output_file)
            print("\n🎉 Экспорт завершен успешно!")
        else:
            print("😞 Товары не найдены")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        print("🔍 Подробности:")
        traceback.print_exc()

if __name__ == "__main__":
    main() 