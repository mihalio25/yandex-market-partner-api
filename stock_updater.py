"""
Скрипт для увеличения остатков товаров в Яндекс.Маркете
Увеличивает остатки всех товаров на указанное количество единиц
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
import time
import csv

from yandex_market_api import (
    YandexMarketClient, 
    AuthType, 
    YandexMarketAPIError,
    create_stock_update_request
)
from config import get_config


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StockUpdater:
    """Класс для обновления остатков товаров"""
    
    def __init__(self, api_key: str, campaign_id: int, dry_run: bool = True):
        """
        Инициализация обновлятора остатков
        
        Args:
            api_key: API ключ
            campaign_id: ID кампании
            dry_run: Тестовый режим (не применять изменения)
        """
        self.api_key = api_key
        self.campaign_id = campaign_id
        self.dry_run = dry_run
        
        # Инициализация клиента API
        self.client = YandexMarketClient(api_key, AuthType.API_KEY)
        
        # Логирование
        self.log_entries = []
        self.start_time = datetime.now()
        
        self.log(f"Инициализация обновлятора остатков - {self.start_time}")
        self.log(f"Режим: {'ТЕСТ' if dry_run else 'ПРОДАКШН'}")
        
        # Получение информации о кампании
        try:
            campaign_info = self.client.campaigns.get_campaign(campaign_id)
            campaign_data = campaign_info.get('campaign', {})
            self.log(f"Кампания: {campaign_data.get('domain', 'N/A')} (ID: {campaign_id})")
        except Exception as e:
            self.log(f"Ошибка получения информации о кампании: {e}")
    
    def log(self, message: str):
        """Логирование с сохранением в память"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        print(log_message)
        self.log_entries.append(log_message)
        logger.info(message)
    
    def get_all_stocks(self) -> List[Dict]:
        """
        Получение всех остатков товаров
        
        Returns:
            Список всех остатков товаров на складах
        """
        self.log("Получение остатков товаров...")
        
        all_stocks = []
        page_token = None
        
        while True:
            try:
                response = self.client.offers.get_warehouse_stocks(
                    campaign_id=self.campaign_id,
                    page_token=page_token,
                    limit=200
                )
                
                result = response.get('result', {})
                warehouses = result.get('warehouses', [])
                
                for warehouse in warehouses:
                    warehouse_id = warehouse.get('warehouseId')
                    offers = warehouse.get('offers', [])
                    
                    for offer in offers:
                        offer_id = offer.get('offerId')
                        stocks = offer.get('stocks', [])
                        
                        for stock in stocks:
                            stock_info = {
                                'offerId': offer_id,
                                'warehouseId': warehouse_id,
                                'type': stock.get('type', 'FIT'),
                                'currentCount': stock.get('count', 0),
                                'updatedAt': offer.get('updatedAt')
                            }
                            all_stocks.append(stock_info)
                
                # Проверяем пагинацию
                paging = result.get('paging', {})
                page_token = paging.get('nextPageToken')
                
                if not page_token:
                    break
                    
                time.sleep(0.5)  # Небольшая задержка между запросами
                
            except Exception as e:
                self.log(f"Ошибка получения остатков: {e}")
                break
        
        self.log(f"Получено остатков: {len(all_stocks)}")
        return all_stocks
    
    def increase_stocks(self, 
                       increase_amount: int = 1,
                       stock_type: str = "FIT",
                       batch_size: int = 50,
                       delay_between_batches: int = 1) -> Dict[str, int]:
        """
        Увеличение остатков товаров
        
        Args:
            increase_amount: На сколько единиц увеличить остатки
            stock_type: Тип остатков для обработки (FIT, DEFECT)
            batch_size: Размер пакета для обновления
            delay_between_batches: Задержка между пакетами в секундах
            
        Returns:
            Статистика обновления
        """
        self.log(f"Начало увеличения остатков на {increase_amount} единиц")
        self.log(f"Тип остатков: {stock_type}")
        
        # Получение текущих остатков
        all_stocks = self.get_all_stocks()
        
        if not all_stocks:
            self.log("Остатки товаров не найдены")
            return {"success": 0, "errors": 0, "skipped": 0}
        
        # Фильтруем по типу остатков
        filtered_stocks = [s for s in all_stocks if s['type'] == stock_type]
        self.log(f"Остатков типа {stock_type}: {len(filtered_stocks)}")
        
        if not filtered_stocks:
            self.log(f"Остатки типа {stock_type} не найдены")
            return {"success": 0, "errors": 0, "skipped": 0}
        
        # Подготовка данных для обновления
        stock_updates = []
        changes_log = []
        
        for stock in filtered_stocks:
            old_count = stock['currentCount']
            new_count = old_count + increase_amount
            
            # Создаем запрос на обновление
            update_request = create_stock_update_request(
                sku=stock['offerId'],
                warehouse_id=stock['warehouseId'],
                count=new_count,
                stock_type=stock['type']
            )
            
            stock_updates.append(update_request)
            
            # Логируем изменение
            change_info = {
                'offerId': stock['offerId'],
                'warehouseId': stock['warehouseId'],
                'type': stock['type'],
                'oldCount': old_count,
                'newCount': new_count,
                'increase': increase_amount
            }
            changes_log.append(change_info)
        
        # Сохранение лога изменений
        self.save_changes_log(changes_log)
        
        # Статистика
        stats = {"success": 0, "errors": 0, "skipped": 0}
        
        if self.dry_run:
            self.log(f"ТЕСТОВЫЙ РЕЖИМ: Запланировано обновление {len(stock_updates)} остатков")
            stats["success"] = len(stock_updates)
        else:
            # Обновление остатков пакетами
            self.log(f"Обновление {len(stock_updates)} остатков пакетами по {batch_size}")
            
            for i in range(0, len(stock_updates), batch_size):
                batch = stock_updates[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    self.log(f"Обработка пакета {batch_num}/{(len(stock_updates) + batch_size - 1) // batch_size}")
                    
                    result = self.client.offers.update_offer_stocks(self.campaign_id, batch)
                    
                    # Проверяем результат
                    if result.get('status') == 'OK':
                        stats["success"] += len(batch)
                        self.log(f"Пакет {batch_num}: успешно обновлено {len(batch)} остатков")
                    else:
                        stats["errors"] += len(batch)
                        self.log(f"Пакет {batch_num}: ошибка - {result}")
                
                except YandexMarketAPIError as e:
                    stats["errors"] += len(batch)
                    self.log(f"Пакет {batch_num}: API ошибка - {e}")
                except Exception as e:
                    stats["errors"] += len(batch)
                    self.log(f"Пакет {batch_num}: неожиданная ошибка - {e}")
                
                # Задержка между пакетами
                if i + batch_size < len(stock_updates) and delay_between_batches > 0:
                    time.sleep(delay_between_batches)
        
        self.log(f"Завершено обновление остатков: успешно {stats['success']}, ошибок {stats['errors']}, пропущено {stats['skipped']}")
        
        # Сохранение лога
        self.save_log()
        
        return stats
    
    def save_changes_log(self, changes: List[Dict]):
        """Сохранение лога изменений в CSV файл"""
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        filename = f"stock_changes_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['offerId', 'warehouseId', 'type', 'oldCount', 'newCount', 'increase']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for change in changes:
                    writer.writerow(change)
            
            self.log(f"Лог изменений сохранен в файл: {filename}")
        except Exception as e:
            self.log(f"Ошибка сохранения лога изменений: {e}")
    
    def save_log(self):
        """Сохранение лога в файл"""
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        filename = f"stock_updates_{timestamp}.log"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for entry in self.log_entries:
                    f.write(entry + '\n')
            
            self.log(f"Лог сохранен в файл: {filename}")
        except Exception as e:
            self.log(f"Ошибка сохранения лога: {e}")


# Пример интерактивного использования
if __name__ == "__main__":
    print("Интерактивный режим обновления остатков товаров")
    print("=" * 50)
    
    # Загружаем конфигурацию из .env файла
    config = get_config()
    config.print_config()
    
    print(f"Используется кампания: {config.campaign_id}")
    
    # Параметры обновления
    try:
        increase_amount = int(input("На сколько единиц увеличить остатки? (по умолчанию 1): ") or "1")
    except ValueError:
        increase_amount = 1
    
    stock_type = input("Тип остатков (FIT/DEFECT, по умолчанию FIT): ").upper() or "FIT"
    if stock_type not in ["FIT", "DEFECT"]:
        stock_type = "FIT"
    
    dry_run_input = input("Запустить в тестовом режиме? (y/n, по умолчанию y): ").lower()
    dry_run = dry_run_input != 'n'
    
    print(f"\nПараметры обновления:")
    print(f"  Увеличение на: {increase_amount} единиц")
    print(f"  Тип остатков: {stock_type}")
    print(f"  Режим: {'ТЕСТ' if dry_run else 'ПРОДАКШН'}")
    
    if not dry_run:
        confirm = input("\n⚠️  ВНИМАНИЕ! Это изменит реальные остатки товаров. Продолжить? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Операция отменена.")
            exit(0)
    
    # Создание и запуск обновлятора
    updater = StockUpdater(config.api_key, config.campaign_id, dry_run)
    
    try:
        stats = updater.increase_stocks(
            increase_amount=increase_amount,
            stock_type=stock_type,
            batch_size=50,
            delay_between_batches=1
        )
        
        print(f"\nРезультат: успешно {stats['success']}, ошибок {stats['errors']}, пропущено {stats['skipped']}")
        
        if dry_run:
            print("\n💡 Это был тестовый запуск. Для реального обновления запустите без тестового режима.")
        elif stats['success'] > 0:
            print("\n✅ Остатки товаров успешно обновлены!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Обновление прервано пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        logger.exception("Неожиданная ошибка") 