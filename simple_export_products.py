#!/usr/bin/env python3
"""
Упрощенный скрипт для быстрого экспорта товаров с ценами
Минимальные настройки, максимальная простота использования
"""

import csv
import time
from datetime import datetime
from yandex_market_api import YandexMarketClient, AuthType
from config import get_config

def export_products_simple(output_file=None):
    """
    Простая функция для экспорта всех товаров с ценами
    
    Args:
        output_file: Имя выходного файла (необязательно)
    """
    # Генерируем имя файла если не указано
    if not output_file:
        output_file = f'products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    print("=" * 50)
    print("📦 ЭКСПОРТ ТОВАРОВ С ЦЕНАМИ")
    print("=" * 50)
    
    try:
        # Загружаем конфигурацию
        config = get_config()
        client = YandexMarketClient(config.api_key, AuthType.API_KEY)
        
        print(f"🏪 Кампания: {config.campaign_id}")
        print(f"📄 Файл: {output_file}")
        print()
        
        # Получаем все товары
        print("🔍 Получаем товары...")
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
            time.sleep(0.1)  # Небольшая пауза
        
        print(f"✅ Всего товаров: {len(all_offers)}")
        
        if not all_offers:
            print("❌ Товары не найдены!")
            return
        
        # Сохраняем в CSV
        print("\n💾 Сохраняем в CSV...")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Заголовки
            writer.writerow([
                'ID товара',
                'Название',
                'Производитель',
                'Цена',
                'Валюта',
                'Статус',
                'Ошибки'
            ])
            
            # Данные
            for offer in all_offers:
                offer_id = offer.get('offerId', '')
                name = offer.get('name', '')
                vendor = offer.get('vendor', '')
                status = offer.get('status', '')
                
                # Извлекаем цену
                price = ''
                currency = ''
                
                # Проверяем разные источники цены
                if offer.get('campaignPrice', {}).get('value'):
                    price = offer['campaignPrice']['value']
                    currency = offer['campaignPrice'].get('currency', 'RUR')
                elif offer.get('basicPrice', {}).get('value'):
                    price = offer['basicPrice']['value']
                    currency = offer['basicPrice'].get('currency', 'RUR')
                
                # Ошибки
                errors = []
                if offer.get('errors'):
                    errors = [e.get('message', '') for e in offer['errors']]
                
                writer.writerow([
                    offer_id,
                    name,
                    vendor,
                    price,
                    currency,
                    status,
                    '; '.join(errors) if errors else ''
                ])
        
        print(f"✅ Данные сохранены в {output_file}")
        
        # Статистика
        with_price = sum(1 for offer in all_offers 
                        if offer.get('campaignPrice', {}).get('value') or 
                           offer.get('basicPrice', {}).get('value'))
        with_errors = sum(1 for offer in all_offers if offer.get('errors'))
        
        print(f"📊 Статистика:")
        print(f"   - Всего товаров: {len(all_offers)}")
        print(f"   - С ценами: {with_price}")
        print(f"   - С ошибками: {with_errors}")
        print()
        print("🎉 Готово!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        print("💡 Проверьте настройки в config.env")


if __name__ == "__main__":
    import sys
    
    # Если передан аргумент - используем как имя файла
    output_file = sys.argv[1] if len(sys.argv) > 1 else None
    
    export_products_simple(output_file) 