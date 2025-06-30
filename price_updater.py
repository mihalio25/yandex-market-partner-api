"""
Скрипт для увеличения цен товаров в Яндекс.Маркете
Поддерживает различные стратегии увеличения цен и безопасные проверки
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from enum import Enum
import argparse
import csv
import os
import math

from yandex_market_api import (
    YandexMarketClient, 
    AuthType, 
    YandexMarketAPIError
)
from config import get_config


class PriceStrategy(Enum):
    """Стратегии увеличения цен"""
    PERCENTAGE = "percentage"  # Увеличение на процент
    FIXED_AMOUNT = "fixed_amount"  # Увеличение на фиксированную сумму
    ROUND_UP = "round_up"  # Округление до красивых цифр
    COMPETITIVE = "competitive"  # Конкурентное ценообразование
    CUSTOM_ROUND = "custom_round"  # Специальное округление до X90


class PriceUpdater:
    """Класс для обновления цен товаров"""
    
    def __init__(self, api_key: str, campaign_id: int, dry_run: bool = True):
        """
        Инициализация обновлятора цен
        
        Args:
            api_key: API ключ Яндекс.Маркета
            campaign_id: Идентификатор кампании
            dry_run: Режим тестирования (не применять изменения)
        """
        self.client = YandexMarketClient(api_key, AuthType.API_KEY)
        self.campaign_id = campaign_id
        self.dry_run = dry_run
        self.log_file = f"price_updates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Инициализация логирования
        self._setup_logging()
        
        # Получение информации о кампании
        self._get_campaign_info()
    
    def _setup_logging(self):
        """Настройка логирования изменений"""
        self.log_entries = []
        self.log(f"Инициализация обновлятора цен - {datetime.now()}")
        self.log(f"Режим: {'ТЕСТ' if self.dry_run else 'ПРОДАКШН'}")
    
    def log(self, message: str):
        """Логирование сообщений"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        self.log_entries.append(log_entry)
    
    def _get_campaign_info(self):
        """Получение информации о кампании"""
        try:
            campaign_response = self.client.campaigns.get_campaign(self.campaign_id)
            self.campaign_info = campaign_response.get('campaign', {})
            self.business_id = self.campaign_info.get('business', {}).get('id')
            
            if not self.business_id:
                raise ValueError("Не удалось получить business_id из кампании")
            
            self.log(f"Кампания: {self.campaign_info.get('domain', 'N/A')} (ID: {self.campaign_id})")
            self.log(f"Бизнес ID: {self.business_id}")
            
        except Exception as e:
            self.log(f"Ошибка получения информации о кампании: {e}")
            raise
    
    def get_offers_with_prices(self, limit: int = 500) -> List[Dict]:
        """
        Получение товаров с ценами
        
        Args:
            limit: Лимит товаров для получения
            
        Returns:
            Список товаров с ценовой информацией
        """
        try:
            self.log("Получение списка товаров...")
            
            # Получаем товары через campaign API с пагинацией
            all_offers = []
            page_token = None
            page_num = 1
            
            while len(all_offers) < limit:
                try:
                    self.log(f"Загрузка страницы {page_num}...")
                    
                    # Вычисляем лимит для этой страницы
                    page_limit = min(200, limit - len(all_offers))  # API позволяет максимум 200 за раз
                    
                    # Получаем товары текущей страницы
                    response = self.client.offers.get_campaign_offers(
                        campaign_id=self.campaign_id,
                        page_token=page_token,
                        limit=page_limit
                    )
                    
                    offers = response.get('result', {}).get('offers', [])
                    if not offers:
                        self.log("Больше товаров не найдено")
                        break
                    
                    all_offers.extend(offers)
                    self.log(f"Найдено {len(offers)} товаров на странице {page_num}")
                    
                    # Проверяем наличие следующей страницы
                    paging = response.get('result', {}).get('paging', {})
                    page_token = paging.get('nextPageToken')
                    
                    if not page_token:
                        self.log("Все страницы обработаны")
                        break
                    
                    page_num += 1
                    
                    # Небольшая задержка между запросами
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.log(f"Ошибка при получении страницы {page_num}: {e}")
                    break
            
            self.log(f"Всего получено товаров: {len(all_offers)}")
            
            # Извлекаем товары с ценами
            offers_with_prices = []
            for offer in all_offers:
                # Проверяем наличие цены
                price = None
                currency = 'RUR'
                
                # Проверяем campaignPrice в первую очередь
                campaign_price = offer.get('campaignPrice', {})
                if campaign_price.get('value'):
                    price = float(campaign_price['value'])
                    currency = campaign_price.get('currency', 'RUR')
                else:
                    # Проверяем basicPrice
                    basic_price = offer.get('basicPrice', {})
                    if basic_price.get('value'):
                        price = float(basic_price['value'])
                        currency = basic_price.get('currency', 'RUR')
                
                if price is not None:
                    offers_with_prices.append({
                        'shopSku': offer.get('offerId'),
                        'name': offer.get('name', 'Без названия'),
                        'category': offer.get('categoryName', 'Без категории'),
                        'current_price': price,
                        'currency': currency,
                        'offer_data': offer
                    })
            
            self.log(f"Товаров с ценами: {len(offers_with_prices)}")
            return offers_with_prices
            
        except Exception as e:
            self.log(f"Ошибка получения товаров: {e}")
            return []
    
    def calculate_new_price(self, 
                          current_price: float, 
                          strategy: PriceStrategy, 
                          value: float,
                          min_price: Optional[float] = None,
                          max_price: Optional[float] = None) -> Tuple[float, str]:
        """
        Расчет новой цены согласно стратегии
        
        Args:
            current_price: Текущая цена
            strategy: Стратегия увеличения
            value: Значение для стратегии
            min_price: Минимальная цена
            max_price: Максимальная цена
            
        Returns:
            Кортеж (новая_цена, описание_изменения)
        """
        if strategy == PriceStrategy.PERCENTAGE:
            new_price = current_price * (1 + value / 100)
            description = f"Увеличение на {value}%"
            
        elif strategy == PriceStrategy.FIXED_AMOUNT:
            new_price = current_price + value
            description = f"Увеличение на {value} руб."
            
        elif strategy == PriceStrategy.ROUND_UP:
            # Округление до красивых цифр
            if current_price < 100:
                new_price = round(current_price * (1 + value / 100), -1)  # До десятков
            elif current_price < 1000:
                new_price = round(current_price * (1 + value / 100), -2)  # До сотен
            else:
                new_price = round(current_price * (1 + value / 100), -3)  # До тысяч
            description = f"Округление с увеличением на {value}%"
            
        elif strategy == PriceStrategy.COMPETITIVE:
            # Конкурентное ценообразование - увеличение с учетом рынка
            multiplier = 1 + (value / 100)
            new_price = current_price * multiplier
            # Округляем до .99 для психологического эффекта
            new_price = int(new_price) + 0.99 if new_price > int(new_price) else new_price
            description = f"Конкурентное увеличение на {value}%"
        
        elif strategy == PriceStrategy.CUSTOM_ROUND:
            # Специальное округление: увеличение на процент, затем округление до X90
            increased_price = current_price * (1 + value / 100)
            new_price = self._custom_round_to_90(increased_price)
            description = f"Увеличение на {value}% с округлением до X90"
        
        else:
            new_price = current_price
            description = "Без изменений"
        
        # Применяем ограничения по цене
        if min_price and new_price < min_price:
            new_price = min_price
            description += f" (ограничено минимумом {min_price})"
        
        if max_price and new_price > max_price:
            new_price = max_price
            description += f" (ограничено максимумом {max_price})"
        
        return new_price, description
    
    def _custom_round_to_90(self, price: float) -> float:
        """
        Специальное округление цены до ближайшего значения, заканчивающегося на 90
        
        Логика:
        base = floor(price / 100)
        lower = base * 100 + 90
        higher = (base + 1) * 100 + 90
        
        Выбирается ближайшее значение
        
        Args:
            price: Исходная цена
            
        Returns:
            Округленная цена
        """
        base = math.floor(price / 100)
        lower = base * 100 + 90
        higher = (base + 1) * 100 + 90
        
        if abs(price - lower) <= abs(price - higher):
            return float(lower)
        else:
            return float(higher)
    
    def apply_price_filters(self, offers: List[Dict], filters: Dict) -> List[Dict]:
        """
        Применение фильтров к товарам
        
        Args:
            offers: Список товаров
            filters: Словарь с фильтрами
            
        Returns:
            Отфильтрованный список товаров
        """
        filtered_offers = offers.copy()
        
        # Фильтр по минимальной цене
        if filters.get('min_current_price'):
            min_price = float(filters['min_current_price'])
            filtered_offers = [o for o in filtered_offers if o['current_price'] >= min_price]
            self.log(f"Фильтр по минимальной цене {min_price}: осталось {len(filtered_offers)} товаров")
        
        # Фильтр по максимальной цене
        if filters.get('max_current_price'):
            max_price = float(filters['max_current_price'])
            filtered_offers = [o for o in filtered_offers if o['current_price'] <= max_price]
            self.log(f"Фильтр по максимальной цене {max_price}: осталось {len(filtered_offers)} товаров")
        
        # Фильтр по категории (частичное совпадение)
        if filters.get('category_filter'):
            category_filter = filters['category_filter'].lower()
            filtered_offers = [o for o in filtered_offers 
                             if category_filter in o['category'].lower()]
            self.log(f"Фильтр по категории '{category_filter}': осталось {len(filtered_offers)} товаров")
        
        # Фильтр по названию товара
        if filters.get('name_filter'):
            name_filter = filters['name_filter'].lower()
            filtered_offers = [o for o in filtered_offers 
                             if name_filter in o['name'].lower()]
            self.log(f"Фильтр по названию '{name_filter}': осталось {len(filtered_offers)} товаров")
        
        # Исключение товаров по SKU
        if filters.get('exclude_skus'):
            exclude_skus = set(filters['exclude_skus'])
            filtered_offers = [o for o in filtered_offers 
                             if str(o['shopSku']) not in exclude_skus]
            self.log(f"Исключены SKU: осталось {len(filtered_offers)} товаров")
        
        return filtered_offers
    
    def update_prices(self, 
                     strategy: PriceStrategy, 
                     value: float,
                     filters: Optional[Dict] = None,
                     batch_size: int = 50,
                     delay_between_batches: int = 1) -> Dict:
        """
        Основной метод обновления цен
        
        Args:
            strategy: Стратегия увеличения цен
            value: Значение для стратегии
            filters: Фильтры для товаров
            batch_size: Размер пакета для обновления
            delay_between_batches: Задержка между пакетами (сек)
            
        Returns:
            Статистика обновлений
        """
        self.log(f"Начало обновления цен - стратегия: {strategy.value}, значение: {value}")
        
        # Получение товаров
        offers = self.get_offers_with_prices()
        if not offers:
            self.log("Нет товаров для обновления")
            return {'success': 0, 'errors': 0, 'skipped': 0}
        
        # Применение фильтров
        if filters:
            offers = self.apply_price_filters(offers, filters)
        
        if not offers:
            self.log("После применения фильтров не осталось товаров")
            return {'success': 0, 'errors': 0, 'skipped': 0}
        
        # Подготовка изменений
        price_updates = []
        changes_log = []
        
        for offer in offers:
            current_price = offer['current_price']
            
            # Расчет новой цены
            new_price, description = self.calculate_new_price(
                current_price, 
                strategy, 
                value
            )
            
            # Проверка на изменение цены
            if abs(new_price - current_price) < 0.01:  # Минимальное изменение 1 копейка
                changes_log.append({
                    'sku': offer['shopSku'],
                    'name': offer['name'],
                    'status': 'SKIPPED',
                    'old_price': current_price,
                    'new_price': new_price,
                    'reason': 'Цена не изменилась'
                })
                continue
            
            # Создание запроса на обновление цены (округляем до 2 знаков)
            price_update = {
                "offerId": offer['shopSku'],
                "price": {
                    "value": round(new_price, 2),
                    "currencyId": offer['currency']
                }
            }
            price_updates.append(price_update)
            
            changes_log.append({
                'sku': offer['shopSku'],
                'name': offer['name'],
                'status': 'PLANNED',
                'old_price': current_price,
                'new_price': new_price,
                'change_amount': new_price - current_price,
                'change_percent': ((new_price - current_price) / current_price) * 100,
                'description': description
            })
        
        # Сохранение лога изменений
        self._save_changes_log(changes_log)
        
        if not price_updates:
            self.log("Нет цен для обновления")
            return {'success': 0, 'errors': 0, 'skipped': len(changes_log)}
        
        # Применение изменений пакетами
        stats = {'success': 0, 'errors': 0, 'skipped': 0}
        
        if self.dry_run:
            self.log(f"ТЕСТОВЫЙ РЕЖИМ: Запланировано обновление {len(price_updates)} цен")
            stats['success'] = len(price_updates)
        else:
            self.log(f"Обновление {len(price_updates)} цен пакетами по {batch_size}")
            
            for i in range(0, len(price_updates), batch_size):
                batch = price_updates[i:i + batch_size]
                self.log(f"Обработка пакета {i//batch_size + 1}: товаров {len(batch)}")
                
                try:
                    # Используем business API вместо campaign API
                    result = self.client.api._make_request(
                        "POST", 
                        f"/businesses/{self.business_id}/offer-prices/updates",
                        data={"offers": batch}
                    )
                    
                    # Анализ результата
                    if result.get('status') == 'OK':
                        stats['success'] += len(batch)
                        self.log(f"Успешно обновлено цен: {len(batch)}")
                    else:
                        stats['errors'] += len(batch)
                        self.log(f"Ошибка обновления пакета: {result}")
                        
                except Exception as e:
                    stats['errors'] += len(batch)
                    self.log(f"Ошибка обновления пакета: {e}")
                
                # Задержка между пакетами
                if i + batch_size < len(price_updates):
                    time.sleep(delay_between_batches)
        
        # Подсчет пропущенных
        stats['skipped'] = len([c for c in changes_log if c['status'] == 'SKIPPED'])
        
        self.log(f"Завершено обновление цен: успешно {stats['success']}, ошибок {stats['errors']}, пропущено {stats['skipped']}")
        
        # Сохранение лога
        self._save_log_file()
        
        return stats
    
    def _save_changes_log(self, changes_log: List[Dict]):
        """Сохранение лога изменений в CSV"""
        if not changes_log:
            return
        
        csv_filename = f"price_changes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['sku', 'name', 'status', 'old_price', 'new_price', 
                            'change_amount', 'change_percent', 'description', 'reason']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for change in changes_log:
                    writer.writerow(change)
            
            self.log(f"Лог изменений сохранен в файл: {csv_filename}")
            
        except Exception as e:
            self.log(f"Ошибка сохранения CSV лога: {e}")
    
    def _save_log_file(self):
        """Сохранение общего лога в файл"""
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(self.log_entries))
            self.log(f"Лог сохранен в файл: {self.log_file}")
        except Exception as e:
            print(f"Ошибка сохранения лога: {e}")


# Пример интерактивного использования
if __name__ == "__main__":
    print("Интерактивный режим обновления цен")
    print("=" * 40)
    
    # Загружаем конфигурацию из .env файла
    config = get_config()
    config.print_config()
    
    print(f"Используется кампания: {config.campaign_id}")
    
    # Выбор стратегии
    print("\nВыберите стратегию увеличения цен:")
    print("1. Процентное увеличение")
    print("2. Фиксированная сумма")
    print("3. Округление с увеличением")
    print("4. Конкурентное ценообразование")
    
    choice = input("Введите номер стратегии (1-4): ")
    strategies = {
        "1": PriceStrategy.PERCENTAGE,
        "2": PriceStrategy.FIXED_AMOUNT,
        "3": PriceStrategy.ROUND_UP,
        "4": PriceStrategy.COMPETITIVE
    }
    
    strategy = strategies.get(choice, PriceStrategy.PERCENTAGE)
    
    # Значение для стратегии
    if strategy in [PriceStrategy.PERCENTAGE, PriceStrategy.ROUND_UP, PriceStrategy.COMPETITIVE]:
        value = float(input("Введите процент увеличения (например, 10 для 10%): "))
    else:
        value = float(input("Введите сумму увеличения в рублях: "))
    
    # Подтверждение
    dry_run = input("Запустить в тестовом режиме? (y/n): ").lower() != 'n'
    
    # Создание и запуск обновлятора
    updater = PriceUpdater(config.api_key, config.campaign_id, dry_run)
    
    try:
        stats = updater.update_prices(strategy=strategy, value=value)
        print(f"\nРезультат: успешно {stats['success']}, ошибок {stats['errors']}, пропущено {stats['skipped']}")
    except Exception as e:
        print(f"Ошибка: {e}")