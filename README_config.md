# Настройка конфигурации

## Быстрый старт

1. **Скопируйте пример конфигурации:**
   ```bash
   cp config.env.example config.env
   ```

2. **Отредактируйте `config.env` файл:**
   ```bash
   nano config.env  # или любой другой редактор
   ```

3. **Заполните реальными данными:**
   ```env
   YANDEX_API_KEY=ACMA:your_real_api_key_here
   YANDEX_CAMPAIGN_ID=137267312
   YANDEX_BUSINESS_ID=187106116
   ```

## Переменные окружения

### Обязательные параметры

| Переменная | Описание | Пример |
|------------|----------|--------|
| `YANDEX_API_KEY` | API ключ Яндекс.Маркета | `ACMA:xxx...` |
| `YANDEX_CAMPAIGN_ID` | ID основной кампании | `137267312` |
| `YANDEX_BUSINESS_ID` | ID бизнеса | `187106116` |

### Дополнительные параметры

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `YANDEX_CAMPAIGN_ID_EXPRES` | ID альтернативной кампании | - |
| `DEFAULT_BATCH_SIZE` | Размер пакета для обновлений | `10` |
| `DEFAULT_DELAY` | Задержка между запросами (сек) | `1` |

## Получение API ключа

1. Войдите в [Партнерский интерфейс Яндекс.Маркета](https://partner.market.yandex.ru/)
2. Перейдите в раздел **Настройки** → **API**
3. Создайте новый API ключ
4. Скопируйте ключ в формате `ACMA:xxxxx...`

## Получение ID кампании и бизнеса

Используйте скрипт для автоматического получения:

```python
from config import get_config
from yandex_market_api import YandexMarketClient, AuthType

# Временно укажите только API ключ
config = get_config()
client = YandexMarketClient("YOUR_API_KEY", AuthType.API_KEY)

# Получите список кампаний
campaigns = client.campaigns.get_campaigns()
for campaign in campaigns.get('campaigns', []):
    print(f"Кампания: {campaign['id']} - {campaign.get('domain', 'N/A')}")
    
    # Получите business_id из кампании
    campaign_details = client.campaigns.get_campaign(campaign['id'])
    business_id = campaign_details.get('campaign', {}).get('business', {}).get('id')
    print(f"Business ID: {business_id}")
```

## Использование в коде

### Автоматическая загрузка конфигурации

```python
from config import get_config

# Конфигурация загружается автоматически
config = get_config()

# Использование
api_key = config.api_key
campaign_id = config.campaign_id
business_id = config.business_id
```

### Выбор кампании

```python
from config import get_config

config = get_config()

# Основная кампания
main_campaign = config.campaign_id

# Альтернативная кампания
expres_campaign = config.get_campaign_id('expres')
```

### Переопределение через переменные окружения

Вы можете переопределить любые настройки через переменные окружения:

```bash
export YANDEX_CAMPAIGN_ID=999999
python3 your_script.py
```

## Безопасность

⚠️ **Важно**: Никогда не добавляйте файл `config.env` в git!

Файл уже добавлен в `.gitignore`, но убедитесь что вы не коммитите конфиденциальные данные:

```bash
# Проверьте статус git
git status

# config.env не должен появляться в списке для коммита
```

## Примеры использования

### CLI с конфигурацией

```bash
# Использует настройки из config.env
python3 price_updater_cli.py --strategy percentage --value 5 --dry-run

# Переопределяет кампанию
python3 price_updater_cli.py --campaign expres --strategy percentage --value 5

# Полное переопределение
python3 price_updater_cli.py --api-key YOUR_KEY --campaign-id 12345 --strategy percentage --value 5
```

### Программное использование

```python
from config import get_config
from yandex_market_api import YandexMarketClient, AuthType

# Автоматическая настройка
config = get_config()
client = YandexMarketClient(config.api_key, AuthType.API_KEY)

# Использование настроек
campaigns = client.campaigns.get_campaigns()
orders = client.orders.get_orders(config.campaign_id)
```

## Устранение проблем

### Ошибка "API ключ не установлен"

```
❌ Обязательная переменная окружения YANDEX_API_KEY не установлена
```

**Решение**: Проверьте файл `config.env` и убедитесь что API ключ указан корректно.

### Ошибка "Файл config.env не найден"

```
⚠️  Файл config.env не найден. Используйте переменные окружения или создайте файл.
```

**Решение**: Скопируйте `config.env.example` в `config.env` и заполните данными.

### Ошибка авторизации API

```
401 Unauthorized
```

**Решение**: Проверьте корректность API ключа и его права доступа в партнерском интерфейсе.

## Миграция со старой версии

Если у вас есть скрипты со встроенными API ключами:

1. Создайте `config.env` файл
2. Перенесите данные в конфигурацию
3. Обновите импорты в коде:

```python
# Старый код
API_KEY = "ACMA:xxx..."
CAMPAIGN_ID = 12345

# Новый код
from config import get_config
config = get_config()
```