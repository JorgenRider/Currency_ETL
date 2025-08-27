# Импорт необходимых модулей AirFlow для создания DAG
from airflow import DAG
# Импорт оператора для выполнения Python функций
from airflow.operators.python import PythonOperator
# Импорт модулей для работы с датой и временем
from datetime import datetime, timedelta
# Импорт базового хука для работы с соединениями AirFlow
from airflow.hooks.base import BaseHook
# Импорт библиотеки для HTTP запросов
import requests
# Импорт библиотеки для парсинга HTML/XML
from bs4 import BeautifulSoup
# Импорт модуля для работы с файловой системой
import os
# Импорт модуля для работы с JSON данными
import json


# Получаем сегодняшнюю дату в формате ДД/ММ/ГГГГ
today = datetime.now().strftime("%d/%m/%Y")


def load_subscribers():
    """Загружает список chat_id из файла."""
    # Проверяем существование файла с подписчиками
    if os.path.exists('/opt/airflow/dags/subscribers.json'):
        # Открываем файл в режиме чтения
        with open('/opt/airflow/dags/subscribers.json', 'r') as f:
            # Загружаем данные из JSON файла
            return json.load(f)
    # Если файл не существует, возвращаем пустой список
    return []


def fetch_currency_rates():
    """Получает текущие курсы валют с сайта Центрального Банка России."""
    
    # Формируем URL для запроса курсов валют на сегодня
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={today}"
    
    # Отправляем GET-запрос к сайту ЦБ
    response = requests.get(url)
    
    # Проверяем, успешен ли запрос (код ответа 200)
    if response.status_code == 200:
        # Разбираем полученный XML-документ
        xml_data = BeautifulSoup(response.text, "xml")
        
        # Список валют, которые нас интересуют
        my_code_list = ['USD', 'EUR', 'IDR']
        
        # Список для хранения строк с курсами валют
        rates = []
        # Переменная для хранения курса USD
        usd_val = None
        # Переменная для хранения курса IDR (сколько рублей за 1 рупию)
        idr_val = None
        
        # Перебираем все элементы Valute в XML
        for currency in xml_data.find_all("Valute"):
            # Проверяем, относится ли валюта к интересующим нас
            if currency.find("CharCode").text in my_code_list:
                # Получаем код валюты (например, USD)
                code = currency.find("CharCode").text
                # Получаем полное название валюты
                name = currency.find("Name").text
                # Получаем номинал (например, для USD это обычно 1, для IDR - 1000)
                nominal = int(currency.find("Nominal").text)
                # Получаем стоимость в рублях, заменяем запятую на точку для преобразования в float
                value = float(currency.find("Value").text.replace(",", "."))
                
                # Рассчитываем курс за 1 единицу валюты
                rate_per_unit = value / nominal
                
                # Сохраняем курс USD для дальнейших расчетов
                if currency.find("CharCode").text == 'USD':
                    usd_val = rate_per_unit

                # Сохраняем курс IDR для дальнейших расчетов
                if currency.find("CharCode").text == 'IDR':
                    # Определяем сколько рупий стоит 1 рубль
                    idr_val = nominal / value
                
                # Формируем строку с курсом в зависимости от его величины
                if rate_per_unit > 1:
                    # Если курс больше 1, показываем сколько стоит 100 единиц в рублях
                    rate_str = f"100 {code}  --> {rate_per_unit * 100:,.2f} RUB.".replace(",", " ")
                    rates.append(rate_str)
                else:
                    # Если курс меньше 1 (в случае с IDR) показываем для разных номиналов
                    # Список номиналов для IDR
                    nom_out_idr = [100000, 250000, 500000, 1000000, 5000000]
                    for nom in nom_out_idr:
                        rate_str = f"{nom:,.2f} IDR --> {rate_per_unit * nom:,.2f} RUB.".replace(",", " ")
                        rates.append(rate_str)

        # Добавляем курс USD к IDR
        rates.append(f"1 USD --> {usd_val * idr_val:,.2f} IDR.".replace(",", " "))
        
        # Объединяем все строки с курсами через перенос строки
        return "\n".join(rates)
    else:
        # В случае ошибки запроса возвращаем сообщение с кодом ошибки
        return f"Ошибка: Не удалось получить данные (HTTP {response.status_code})"


def send_telegram_notification(**context):
    """Отправка сообщения через Generic Connection (с вашими курсами валют)."""
    # Получаем данные из функции fetch_currency_rates через XCom
    rates = context['ti'].xcom_pull(task_ids='fetch_rates')  # Важно: task_id должен совпадать
    # Загружаем список подписчиков из файла
    subscribers = load_subscribers()
    
    # Достаём настройки из Connection
    conn = BaseHook.get_connection('telegram_generic')
    # Получаем дополнительные параметры соединения
    extra = conn.extra_dejson

    # Отправляем сообщение каждому подписчику
    for chat_id in subscribers:
        # Формируем URL для отправки сообщения
        url = f"https://api.telegram.org/bot{extra['token']}/sendMessage"
    
        # Отправляем POST запрос с сообщением
        requests.post(
            url,
            json={
                'chat_id': chat_id,  # Используется chat_id из списка подписчиков
                'text': f"Курсы валют на {today}:\n\n{rates}",
                'parse_mode': 'HTML'
            }
        )
    

# Создание DAG для работы с курсами валют
with DAG(
    # Уникальный идентификатор DAG
    dag_id='currency_with_telegram',
    # Описание DAG для интерфейса AirFlow
    description='Сбор курсов валют с уведомлением в Telegram',
    # Расписание выполнения: каждый день в 20:30
    schedule='30 20 * * *',
    # Дата начала работы DAG
    start_date=datetime(2025, 7, 29),
    # Отключение дозапуска пропущенных интервалов
    catchup=False,
    # Аргументы по умолчанию для всех задач в DAG
    default_args={
        # Количество повторных попыток при ошибке
        'retries': 2,
        # Задержка между попытками
        'retry_delay': timedelta(minutes=3),
        # Функция обратного вызова при неудаче задачи
        'on_failure_callback': lambda context: print("Task failed!"),
    },
) as dag:

    # Задача для получения курсов валют
    fetch_task = PythonOperator(
        # Уникальный идентификатор задачи
        task_id='fetch_rates',
        # Python функция для выполнения
        python_callable=fetch_currency_rates,
    )

    # Задача для отправки уведомлений
    send_task = PythonOperator(
        # Уникальный идентификатор задачи
        task_id='send_notification',
        # Python функция для выполнения
        python_callable=send_telegram_notification,
    )

    # Определение порядка выполнения задач
    fetch_task >> send_task