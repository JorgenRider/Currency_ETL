# Импорт необходимых модулей AirFlow для создания DAG и задач
from airflow import DAG
# Импорт оператора для выполнения Python функций
from airflow.operators.python import PythonOperator
# Импорт модулей для работы с датой и временем
from datetime import datetime, timedelta
# Импорт библиотеки для HTTP запросов
import requests
# Импорт модуля для работы с JSON данными
import json
# Импорт модуля для работы с файловой системой
import os
# Импорт базового хука для работы с соединениями AirFlow
from airflow.hooks.base import BaseHook
# Импорт модуля для файловых блокировок (для Linux систем)
import fcntl

# Константа с путем к файлу для хранения подписчиков
# Файл будет создан в той же директории, где находится DAG файл
SUBSCRIBERS_FILE = '/opt/airflow/dags/subscribers.json'

def load_subscribers():
    """
    Загружает список chat_id подписчиков из JSON файла.
    Использует блокировку файла для предотвращения race condition.
    """
    # Проверяем существование файла перед открытием
    if os.path.exists(SUBSCRIBERS_FILE):
        # Открываем файл в режиме чтения
        with open(SUBSCRIBERS_FILE, 'r') as f:
            # Устанавливаем разделяемую блокировку для чтения (могут читать другие процессы)
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                # Загружаем данные из JSON файла
                data = json.load(f)
                # Возвращаем загруженные данные
                return data
            finally:
                # Снимаем блокировку файла в блоке finally для гарантированного выполнения
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    # Если файл не существует, возвращаем пустой список
    return []

def save_subscribers(subscribers):
    """
    Сохраняет список подписчиков в JSON файл.
    Использует эксклюзивную блокировку для безопасной записи.
    """
    # Создаем директорию для файла, если она не существует
    os.makedirs(os.path.dirname(SUBSCRIBERS_FILE), exist_ok=True)
    # Открываем файл в режиме записи
    with open(SUBSCRIBERS_FILE, 'w') as f:
        # Устанавливаем эксклюзивную блокировку для записи (только один процесс может писать)
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            # Записываем данные в файл в формате JSON
            json.dump(subscribers, f)
        finally:
            # Снимаем блокировку файла после записи
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def process_telegram_updates():
    """
    Основная функция для обработки обновлений от Telegram бота.
    Получает новые сообщения, обрабатывает команды /start и /stop.
    """
    # Получаем соединение с Telegram из AirFlow connections
    conn = BaseHook.get_connection('telegram_generic')
    # Извлекаем токен бота из дополнительных параметров соединения
    token = conn.extra_dejson['token']
    
    # Формируем URL для получения обновлений от Telegram API
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    # Выполняем GET запрос к Telegram API
    response = requests.get(url)
    
    # Проверяем успешность HTTP запроса
    if response.status_code == 200:
        # Извлекаем список обновлений из ответа API
        updates = response.json().get('result', [])
        # Загружаем текущий список подписчиков из файла
        subscribers = load_subscribers()
        
        # Проходим по всем полученным обновлениям
        for update in updates:
            # Извлекаем объект сообщения из обновления
            message = update.get('message', {})
            # Извлекаем текст сообщения и приводим к нижнему регистру
            text = message.get('text', '').strip().lower()
            # Извлекаем chat_id из информации о чате
            chat_id = message.get('chat', {}).get('id')
            
            # Пропускаем обработку, если chat_id отсутствует
            if not chat_id:
                # Переходим к следующему обновлению
                continue
                
            # Обрабатываем команду /start для подписки
            if text == '/start':
                # Проверяем, что пользователь еще не в списке подписчиков
                if chat_id not in subscribers:
                    # Добавляем нового подписчика в список
                    subscribers.append(chat_id)
                    # Сохраняем обновленный список подписчиков
                    save_subscribers(subscribers)
                    # Отправляем подтверждение о успешной подписке
                    send_message(chat_id, "✅ Вы подписались на курсы валют!")
                    
            # Обрабатываем команду /stop для отписки
            elif text == '/stop':
                # Проверяем, что пользователь есть в списке подписчиков
                if chat_id in subscribers:
                    # Удаляем пользователя из списка подписчиков
                    subscribers.remove(chat_id)
                    # Сохраняем обновленный список подписчиков
                    save_subscribers(subscribers)
                    # Отправляем подтверждение об отписке
                    send_message(chat_id, "❌ Вы отписались от рассылки")
        
        # Помечаем обработанные обновления, чтобы не получать их повторно
        if updates:
            # Получаем ID последнего обработанного обновления
            last_update_id = updates[-1]['update_id']
            # Отправляем запрос к API Telegram для смещения offset
            requests.get(f"https://api.telegram.org/bot{token}/getUpdates?offset={last_update_id + 1}")

def send_message(chat_id, text):
    """
    Отправляет текстовое сообщение в указанный чат Telegram.
    """
    # Получаем соединение с Telegram из AirFlow connections
    conn = BaseHook.get_connection('telegram_generic')
    # Извлекаем токен бота из дополнительных параметров соединения
    token = conn.extra_dejson['token']
    
    # Формируем URL для отправки сообщения через Telegram API
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Выполняем POST запрос с данными сообщения в формате JSON
    requests.post(url, json={'chat_id': chat_id, 'text': text})

# Создание DAG (Directed Acyclic Graph) - основного объекта AirFlow
with DAG(
    # Уникальный идентификатор DAG
    dag_id='telegram_subscription_bot',
    # Описание DAG для интерфейса AirFlow
    description='Обработка подписок на рассылку курсов валют',
    # Расписание выполнения: каждые 10 минут
    schedule='*/5 * * * *',
    # Дата начала работы DAG
    start_date=datetime(2025, 8, 26),
    # Отключение дозапуска пропущенных интервалов
    catchup=False,
    # Аргументы по умолчанию для всех задач в DAG
    default_args={
        # Количество повторных попыток при ошибке
        'retries': 1,
        # Задержка между попытками
        'retry_delay': timedelta(minutes=1),
    },
) as dag:

    # Создание задачи PythonOperator для обработки обновлений
    process_updates_task = PythonOperator(
        # Уникальный идентификатор задачи внутри DAG
        task_id='process_telegram_updates',
        # Python функция, которая будет выполнена
        python_callable=process_telegram_updates,
    )