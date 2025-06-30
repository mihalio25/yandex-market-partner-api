"""
Модуль для работы с конфигурацией из переменных окружения
"""

import os
from typing import Optional


def load_env_file(filename: str = "config.env") -> None:
    """
    Загружает переменные окружения из файла
    
    Args:
        filename: Имя файла с переменными окружения
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    except FileNotFoundError:
        print(f"⚠️  Файл {filename} не найден. Используйте переменные окружения или создайте файл.")
    except Exception as e:
        print(f"❌ Ошибка загрузки {filename}: {e}")


class Config:
    """Класс для работы с конфигурацией приложения"""
    
    def __init__(self, env_file: str = "config.env"):
        """
        Инициализация конфигурации
        
        Args:
            env_file: Путь к файлу с переменными окружения
        """
        # Загружаем переменные из файла
        load_env_file(env_file)
        
        # API настройки
        self.api_key = self._get_required("YANDEX_API_KEY")
        self.campaign_id = int(self._get_required("YANDEX_CAMPAIGN_ID"))
        self.business_id = int(self._get_required("YANDEX_BUSINESS_ID"))
        
        # Альтернативные кампании
        self.campaign_id_expres = self._get_optional("YANDEX_CAMPAIGN_ID_EXPRES", int)
        
        # Настройки по умолчанию
        self.default_batch_size = self._get_optional("DEFAULT_BATCH_SIZE", int, 10)
        self.default_delay = self._get_optional("DEFAULT_DELAY", int, 1)
    
    def _get_required(self, key: str) -> str:
        """Получает обязательную переменную окружения"""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"❌ Обязательная переменная окружения {key} не установлена")
        return value
    
    def _get_optional(self, key: str, type_func=str, default=None):
        """Получает необязательную переменную окружения"""
        value = os.getenv(key)
        if value is None:
            return default
        
        if type_func == int:
            try:
                return int(value)
            except ValueError:
                print(f"⚠️  Неверный формат для {key}: {value}. Используется значение по умолчанию: {default}")
                return default
        
        return type_func(value) if type_func != str else value
    
    def get_campaign_id(self, campaign_name: Optional[str] = None) -> int:
        """
        Получает ID кампании по имени или возвращает основную
        
        Args:
            campaign_name: Имя кампании ('expres' для альтернативной)
            
        Returns:
            ID кампании
        """
        if campaign_name and campaign_name.lower() == 'expres':
            if self.campaign_id_expres:
                return self.campaign_id_expres
            else:
                print("⚠️  Кампания 'expres' не настроена. Используется основная кампания.")
        
        return self.campaign_id
    
    def print_config(self) -> None:
        """Выводит текущую конфигурацию (без API ключа)"""
        print("=== ТЕКУЩАЯ КОНФИГУРАЦИЯ ===")
        print(f"API ключ: {self.api_key[:20]}...")
        print(f"Основная кампания ID: {self.campaign_id}")
        print(f"Бизнес ID: {self.business_id}")
        if self.campaign_id_expres:
            print(f"Альтернативная кампания (Expres): {self.campaign_id_expres}")
        print(f"Размер пакета по умолчанию: {self.default_batch_size}")
        print(f"Задержка по умолчанию: {self.default_delay} сек")
        print("=" * 30)


# Глобальный экземпляр конфигурации
config = Config()


def get_config() -> Config:
    """Возвращает глобальный экземпляр конфигурации"""
    return config 