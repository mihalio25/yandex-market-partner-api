#!/usr/bin/env python3
"""
CLI скрипт для увеличения цен товаров в Яндекс.Маркете
Поддерживает различные стратегии увеличения цен и фильтры
"""

import argparse
import sys
from price_updater import PriceUpdater, PriceStrategy
from config import get_config


def main():
    """Основная функция с CLI интерфейсом"""
    parser = argparse.ArgumentParser(
        description='Обновление цен товаров в Яндекс.Маркете',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Увеличить все цены на 10% в тестовом режиме:
   python3 price_updater_cli.py --api-key YOUR_KEY --campaign-id 12345 --strategy percentage --value 10 --dry-run

2. Увеличить цены на 100 рублей для товаров дороже 1000 рублей:
   python3 price_updater_cli.py --api-key YOUR_KEY --campaign-id 12345 --strategy fixed_amount --value 100 --min-price 1000

3. Конкурентное увеличение на 15% для категории "Сетевые зарядные устройства":
   python3 price_updater_cli.py --api-key YOUR_KEY --campaign-id 12345 --strategy competitive --value 15 --category "Сетевые зарядные"

4. Округление цен с увеличением на 5% для товаров от 500 до 5000 рублей:
   python3 price_updater_cli.py --api-key YOUR_KEY --campaign-id 12345 --strategy round_up --value 5 --min-price 500 --max-price 5000

5. Увеличение цен на 8.5% с округлением до X90 (например, 1250 -> 1290):
   python3 price_updater_cli.py --strategy custom_round --value 8.5
        """
    )
    
    # Основные параметры
    parser.add_argument('--api-key', 
                       help='API ключ Яндекс.Маркета (по умолчанию из config.env)')
    parser.add_argument('--campaign-id', type=int, 
                       help='ID кампании (по умолчанию из config.env)')
    parser.add_argument('--campaign', choices=['main', 'expres'],
                       help='Выбор кампании: main (основная) или expres')
    parser.add_argument('--strategy', 
                       choices=['percentage', 'fixed_amount', 'round_up', 'competitive', 'custom_round'], 
                       default='percentage', 
                       help='Стратегия увеличения цен (по умолчанию: percentage)')
    parser.add_argument('--value', type=float, required=True, 
                       help='Значение для стратегии (процент или сумма)')
    
    # Фильтры товаров
    filter_group = parser.add_argument_group('Фильтры товаров')
    filter_group.add_argument('--min-price', type=float, 
                             help='Минимальная текущая цена товара')
    filter_group.add_argument('--max-price', type=float, 
                             help='Максимальная текущая цена товара')
    filter_group.add_argument('--category', 
                             help='Фильтр по категории (частичное совпадение)')
    filter_group.add_argument('--name-filter', 
                             help='Фильтр по названию товара')
    filter_group.add_argument('--exclude-skus', nargs='+', 
                             help='Исключить товары по SKU/offerId')
    
    # Ограничения новых цен
    limit_group = parser.add_argument_group('Ограничения новых цен')
    limit_group.add_argument('--min-new-price', type=float, 
                            help='Минимальная новая цена')
    limit_group.add_argument('--max-new-price', type=float, 
                            help='Максимальная новая цена')
    
    # Настройки выполнения
    exec_group = parser.add_argument_group('Настройки выполнения')
    exec_group.add_argument('--batch-size', type=int, default=50, 
                           help='Размер пакета для обновления (по умолчанию: 50)')
    exec_group.add_argument('--delay', type=int, default=1, 
                           help='Задержка между пакетами в секундах (по умолчанию: 1)')
    exec_group.add_argument('--dry-run', action='store_true', 
                           help='Тестовый режим (не применять изменения)')
    exec_group.add_argument('--limit', type=int, default=500,
                           help='Максимальное количество товаров для обработки (по умолчанию: 500)')
    
    # Дополнительные опции
    misc_group = parser.add_argument_group('Дополнительные опции')
    misc_group.add_argument('--verbose', '-v', action='store_true',
                           help='Подробный вывод')
    misc_group.add_argument('--quiet', '-q', action='store_true',
                           help='Минимальный вывод')
    
    args = parser.parse_args()
    
    # Проверка совместимости аргументов
    if args.verbose and args.quiet:
        parser.error("--verbose и --quiet нельзя использовать одновременно")
    
    if args.min_price and args.max_price and args.min_price > args.max_price:
        parser.error("--min-price не может быть больше --max-price")
    
    if args.min_new_price and args.max_new_price and args.min_new_price > args.max_new_price:
        parser.error("--min-new-price не может быть больше --max-new-price")
    
    # Подготовка фильтров
    filters = {}
    if args.min_price:
        filters['min_current_price'] = args.min_price
    if args.max_price:
        filters['max_current_price'] = args.max_price
    if args.category:
        filters['category_filter'] = args.category
    if args.name_filter:
        filters['name_filter'] = args.name_filter
    if args.exclude_skus:
        filters['exclude_skus'] = args.exclude_skus
    if args.min_new_price:
        filters['min_new_price'] = args.min_new_price
    if args.max_new_price:
        filters['max_new_price'] = args.max_new_price
    
    # Загружаем конфигурацию
    config = get_config()
    
    # Определяем параметры подключения
    api_key = args.api_key or config.api_key
    
    if args.campaign_id:
        campaign_id = args.campaign_id
    elif args.campaign:
        campaign_id = config.get_campaign_id(args.campaign)
    else:
        campaign_id = config.campaign_id
    
    if not api_key:
        print("❌ API ключ не указан. Используйте --api-key или config.env", file=sys.stderr)
        return 1
    
    # Создание обновлятора
    try:
        updater = PriceUpdater(api_key, campaign_id, args.dry_run)
        
        # Настройка уровня логирования
        if args.quiet:
            # Перенаправляем логи только в файл
            original_log = updater.log
            def quiet_log(message):
                updater.log_entries.append(f"[{updater.log_entries[-1].split(']')[0][1:]}] {message}" if updater.log_entries else message)
            updater.log = quiet_log
        
    except Exception as e:
        print(f"Ошибка инициализации: {e}", file=sys.stderr)
        return 1
    
    # Выполнение обновления
    try:
        if not args.quiet:
            print(f"Запуск обновления цен:")
            print(f"  Стратегия: {args.strategy}")
            print(f"  Значение: {args.value}")
            print(f"  Режим: {'ТЕСТ' if args.dry_run else 'ПРОДАКШН'}")
            if filters:
                print(f"  Фильтры: {len(filters)} активных")
            print("-" * 50)
        
        strategy = PriceStrategy(args.strategy)
        stats = updater.update_prices(
            strategy=strategy,
            value=args.value,
            filters=filters if filters else None,
            batch_size=args.batch_size,
            delay_between_batches=args.delay
        )
        
        # Вывод результатов
        if args.quiet:
            print(f"{stats['success']},{stats['errors']},{stats['skipped']}")
        else:
            print(f"\n{'='*50}")
            print(f"РЕЗУЛЬТАТ ОБНОВЛЕНИЯ ЦЕН")
            print(f"{'='*50}")
            print(f"Успешно обновлено: {stats['success']}")
            print(f"Ошибок: {stats['errors']}")
            print(f"Пропущено: {stats['skipped']}")
            print(f"{'='*50}")
            
            if args.dry_run:
                print("⚠️  Это был тестовый запуск. Цены не изменены.")
            elif stats['success'] > 0:
                print("✅ Цены успешно обновлены!")
            
            if stats['errors'] > 0:
                print("❌ Обнаружены ошибки. Проверьте лог-файлы.")
        
        return 0 if stats['errors'] == 0 else 1
        
    except KeyboardInterrupt:
        if not args.quiet:
            print("\n⚠️  Обновление прервано пользователем")
        return 130
    except Exception as e:
        print(f"Ошибка выполнения: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 