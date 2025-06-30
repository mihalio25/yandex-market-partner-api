#!/usr/bin/env python3
"""
Детальный экспорт цен товаров через Yandex Market Partner API
Получает актуальные цены через отдельные API вызовы
"""

import csv
import time
import json
from datetime import datetime
from typing import Dict, List
from yandex_market_api import YandexMarketClient, AuthType
from config import get_config

def get_detailed_prices(client: YandexMarketClient, campaign_id: int, offer_ids: List[str]) -> Dict:
    """
    Получение детальных цен для списка товаров
    
    Args:
        client: API клиент
        campaign_id: ID кампании
        offer_ids: Список ID товаров
        
    Returns:
        Словарь с данными о ценах
    """
    if not offer_ids:
        return {}
    
    print(f"💰 Получение детальных цен для {len(offer_ids)} товаров...")
    
    # Разбиваем на пакеты по 1000 товаров
    batch_size = 1000
    all_prices = {}
    
    for i in range(0, len(offer_ids), batch_size):
        batch = offer_ids[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(offer_ids) + batch_size - 1) // batch_size
        
        print(f"   Пакет {batch_num}/{total_batches}: {len(batch)} товаров")
        
        try:
            # Делаем запрос к API цен
            request_body = {"offerIds": batch}
            
            response = client.api._make_request(
                "POST", 
                f"/campaigns/{campaign_id}/offer-prices",
                data=request_body
            )
            
            # Обрабатываем ответ
            for offer_data in response.get('offers', []):
                offer_id = offer_data.get('offerId')
                if offer_id:
                    all_prices[offer_id] = offer_data
            
            # Пауза между пакетами
            if i + batch_size < len(offer_ids):
                time.sleep(0.2)
                
        except Exception as e:
            print(f"   ⚠️ Ошибка в пакете {batch_num}: {e}")
            continue
    
    print(f"✅ Получено цен для {len(all_prices)} товаров")
    return all_prices

def export_detailed_prices(output_file=None):
    """
    Экспорт товаров с детальными ценами
    
    Args:
        output_file: Имя выходного файла
    """
    if not output_file:
        output_file = f'detailed_prices_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    print("=" * 60)
    print("💎 ДЕТАЛЬНЫЙ ЭКСПОРТ ЦЕН ТОВАРОВ")
    print("=" * 60)
    
    try:
        # Инициализация
        config = get_config()
        client = YandexMarketClient(config.api_key, AuthType.API_KEY)
        
        print(f"🏪 Кампания: {config.campaign_id}")
        print(f"📄 Файл: {output_file}")
        print()
        
        # 1. Получаем все товары
        print("🔍 Получение списка товаров...")
        all_offers = []
        page_token = None
        page = 1
        
        while True:
            print(f"   Страница {page}...", end=" ")
            
            response = client.offers.get_offers(
                campaign_id=config.campaign_id,
                page_token=page_token,
                limit=200
            )
            
            offers = response.get('offers', [])
            if not offers:
                print("Готово!")
                break
            
            all_offers.extend(offers)
            print(f"найдено {len(offers)} товаров")
            
            paging = response.get('paging', {})
            page_token = paging.get('nextPageToken')
            if not page_token:
                break
                
            page += 1
            time.sleep(0.1)
        
        print(f"✅ Всего товаров: {len(all_offers)}")
        
        if not all_offers:
            print("❌ Товары не найдены!")
            return
        
        # 2. Получаем детальные цены
        offer_ids = [offer.get('offerId') for offer in all_offers if offer.get('offerId')]
        detailed_prices = get_detailed_prices(client, config.campaign_id, offer_ids)
        
        # 3. Сохраняем в CSV
        print("\n💾 Сохранение в CSV...")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Заголовки
            writer.writerow([
                'ID товара',
                'Название',
                'Производитель',
                'Цена в кампании',
                'Базовая цена',
                'Актуальная цена',
                'Валюта',
                'Дата обновления цены',
                'Статус',
                'ID категории',
                'Количество ошибок',
                'Ошибки',
                'Предупреждения'
            ])
            
            # Данные
            for offer in all_offers:
                offer_id = offer.get('offerId', '')
                name = offer.get('name', '')
                vendor = offer.get('vendor', '')
                status = offer.get('status', '')
                category_id = offer.get('marketCategoryId', '')
                
                # Цены из основных данных
                campaign_price = ''
                basic_price = ''
                currency = 'RUR'
                
                if offer.get('campaignPrice', {}).get('value'):
                    campaign_price = offer['campaignPrice']['value']
                    currency = offer['campaignPrice'].get('currency', 'RUR')
                
                if offer.get('basicPrice', {}).get('value'):
                    basic_price = offer['basicPrice']['value']
                
                # Актуальная цена из детального запроса
                actual_price = ''
                price_updated = ''
                
                if offer_id in detailed_prices:
                    price_info = detailed_prices[offer_id]
                    price_data = price_info.get('price', {})
                    if price_data.get('value'):
                        actual_price = price_data['value']
                        currency = price_data.get('currency', currency)
                    price_updated = price_info.get('updatedAt', '')
                
                # Ошибки и предупреждения  
                errors = []
                warnings = []
                
                if offer.get('errors'):
                    errors = [e.get('message', '') for e in offer['errors']]
                
                if offer.get('warnings'):
                    warnings = [w.get('message', '') for w in offer['warnings']]
                
                writer.writerow([
                    offer_id,
                    name,
                    vendor,
                    campaign_price,
                    basic_price,
                    actual_price,
                    currency,
                    price_updated,
                    status,
                    category_id,
                    len(errors),
                    '; '.join(errors) if errors else '',
                    '; '.join(warnings) if warnings else ''
                ])
        
        print(f"✅ Данные сохранены в {output_file}")
        
        # Статистика
        with_campaign_price = sum(1 for offer in all_offers 
                                 if offer.get('campaignPrice', {}).get('value'))
        with_basic_price = sum(1 for offer in all_offers 
                              if offer.get('basicPrice', {}).get('value'))
        with_actual_price = len(detailed_prices)
        with_errors = sum(1 for offer in all_offers if offer.get('errors'))
        
        print(f"\n📊 Статистика:")
        print(f"   - Всего товаров: {len(all_offers)}")
        print(f"   - С ценами кампании: {with_campaign_price}")
        print(f"   - С базовыми ценами: {with_basic_price}")
        print(f"   - С актуальными ценами: {with_actual_price}")
        print(f"   - С ошибками: {with_errors}")
        print()
        print("🎉 Готово!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        print("💡 Проверьте настройки в config.env")
        import traceback
        print("🔍 Подробности ошибки:")
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    # Если передан аргумент - используем как имя файла
    output_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    export_detailed_prices(output_file) 