import os
import aio_pika
import asyncio
import aiohttp
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import json

# Настройка логирования
logger = logging.getLogger('crawler')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('crawler.log', encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)

# Путь к файлу, где будут храниться обработанные ссылки
processed_urls_file = 'processed_urls.json'


# Функция для чтения обработанных ссылок из файла
def read_processed_urls():
    if os.path.exists(processed_urls_file):
        with open(processed_urls_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


# Функция для записи обработанных ссылок в файл
def write_processed_urls(processed_urls):
    with open(processed_urls_file, 'w', encoding='utf-8') as f:
        json.dump(processed_urls, f, ensure_ascii=False, indent=4)


# Функция для получения всех внутренних ссылок на странице
async def get_internal_links(url, base_url, processed_urls):
    async with aiohttp.ClientSession() as session:
        try:
            # Добавляем схему, если отсутствует
            if not urlparse(url).scheme:
                url = f"http:/{url}"

            async with session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                # Логируем название страницы
                page_title = soup.title.string if soup.title else "Без названия"
                logger.info(f"Обрабатывается страница: {page_title} ({url})")

                links = []
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']

                    # Пропускаем некорректные или пустые ссылки
                    if not href or href.startswith('#'):
                        continue

                    # Формируем абсолютную ссылку
                    if not urlparse(href).scheme:
                        href = urljoin(url, href)

                    # Фильтруем внутренние ссылки
                    if urlparse(href).netloc == base_url:
                        if href not in processed_urls:  # Проверяем, была ли ссылка уже обработана
                            link_info = {
                                "text": a_tag.get_text(strip=True),  # Текст внутри тега <a>
                                "url": href  # Абсолютная ссылка
                            }
                            links.append(link_info)

                            # Логируем найденную ссылку
                            logger.info(f"Найдена ссылка: {link_info['text'] or 'Без названия'} ({link_info['url']})")
                            processed_urls[href] = "processed"  # Отмечаем ссылку как обработанную

                # Сохраняем изменения в файл
                write_processed_urls(processed_urls)

                return links
        except Exception as e:
            logger.error(f"Ошибка при обработке {url}: {e}")
            return []


async def process_message(message: aio_pika.IncomingMessage, exchange, processed_urls):
    async with message.process():
        url = message.body.decode()
        base_url = urlparse(url).netloc

        # Получаем ссылки на странице
        links = await get_internal_links(url, base_url, processed_urls)

        # Добавляем найденные ссылки обратно в очередь
        for link in links:
            try:
                await exchange.publish(
                    aio_pika.Message(body=link['url'].encode()),
                    routing_key="urls"
                )
                logger.info(f"Добавлена в очередь ссылка: {link['url']}")
            except Exception as e:
                logger.error(f"Ошибка при добавлении ссылки {link['url']}: {e}")


# Асинхронная функция для подключения и обработки очереди RabbitMQ
async def consume():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)

    # Читаем список обработанных ссылок
    processed_urls = read_processed_urls()

    connection = await aio_pika.connect_robust(f"amqp://{rabbitmq_host}:{rabbitmq_port}/")

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        # Декларируем обмен и очередь
        exchange = await channel.declare_exchange("urls_exchange", aio_pika.ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue("urls", durable=True)
        await queue.bind(exchange, routing_key="urls")

        # Начинаем потребление сообщений
        await queue.consume(lambda message: process_message(message, exchange, processed_urls))

        logger.info("Ожидание сообщений...")
        await asyncio.Future()  # Для непрерывной работы


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume())
