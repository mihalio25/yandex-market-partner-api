"""
Отладочный скрипт для анализа структуры данных товаров
"""

import json
from yandex_market_api import YandexMarketClient, AuthType

# Параметры подключения
API_KEY = "ACMA:mF18at8ZlpwlTVWv9L3dyEZs62QwEouVRPGnoCs5:8a889873"
CAMPAIGN_ID = 137267312

# Создание клиента
client = YandexMarketClient(API_KEY, AuthType.API_KEY)

# Получение информации о кампании
campaign_response = client.campaigns.get_campaign(CAMPAIGN_ID)
campaign_info = campaign_response.get('campaign', {})
business_id = campaign_info.get('business', {}).get('id')

print(f"Кампания: {campaign_info.get('domain', 'N/A')} (ID: {CAMPAIGN_ID})")
print(f"Бизнес ID: {business_id}")

# Получение товаров
params = {"limit": 5}  # Ограничиваем до 5 товаров для анализа
data = {}

offers_response = client.api._make_request(
    "POST", 
    f"/businesses/{business_id}/offer-mappings", 
    params=params, 
    data=data
)

offer_mappings = offers_response.get('result', {}).get('offerMappings', [])
print(f"\nПолучено товаров для анализа: {len(offer_mappings)}")

# Анализ структуры первых нескольких товаров
for i, mapping in enumerate(offer_mappings[:3]):
    print(f"\n{'='*50}")
    print(f"ТОВАР #{i+1}")
    print(f"{'='*50}")
    
    offer = mapping.get('offer', {})
    
    # Основная информация
    print(f"SKU: {offer.get('shopSku', 'N/A')}")
    print(f"Название: {offer.get('name', 'N/A')}")
    print(f"Категория: {offer.get('category', 'N/A')}")
    
    # Проверяем все ключи в offer
    print(f"\nВсе ключи в offer:")
    for key in sorted(offer.keys()):
        value = offer[key]
        if isinstance(value, dict):
            print(f"  {key}: {type(value)} с ключами {list(value.keys())}")
        elif isinstance(value, list):
            print(f"  {key}: {type(value)} длиной {len(value)}")
        else:
            print(f"  {key}: {value}")
    
    # Проверяем mapping
    print(f"\nВсе ключи в mapping:")
    for key in sorted(mapping.keys()):
        value = mapping[key]
        if isinstance(value, dict):
            print(f"  {key}: {type(value)} с ключами {list(value.keys())}")
        elif isinstance(value, list):
            print(f"  {key}: {type(value)} длиной {len(value)}")
        else:
            print(f"  {key}: {value}")
    
    # Полная структура первого товара
    if i == 0:
        print(f"\nПОЛНАЯ СТРУКТУРА ПЕРВОГО ТОВАРА:")
        print(json.dumps(mapping, indent=2, ensure_ascii=False))

print(f"\n{'='*50}")
print("АНАЛИЗ ЗАВЕРШЕН")
print(f"{'='*50}") 