import sys
import os
import pika
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin


# Функция для получения всех внутренних ссылок на странице
async def get_internal_links(url, base_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Если ссылка абсолютная, добавляем ее как есть
                if href.startswith('http') or href.startswith('https'):
                    if base_url in href:
                        links.append(href)
                # Если ссылка относительная, делаем ее абсолютной
                else:
                    full_url = urljoin(url, href)
                    links.append(full_url)
            return links


# Функция для подключения к RabbitMQ и отправки ссылок
def send_to_queue(links, queue_name):
    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST', 'localhost')))
    channel = connection.channel()

    # Декларация очереди с параметром durable=True
    # Это гарантирует, что очередь будет существовать после перезапуска RabbitMQ
    channel.queue_declare(queue=queue_name, durable=True)

    # Отправка сообщений в очередь
    for link in links:
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=link,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Сделать сообщение стойким (persistent)
            )
        )
        print(f"Sent: {link}")

    # Закрытие соединения
    connection.close()


async def main():
    if len(sys.argv) < 2:
        print("Usage: producer.py <URL>")
        return

    url = sys.argv[1]
    base_url = urlparse(url).netloc

    print(f"Processing {url}")

    # Получаем все внутренние ссылки
    links = await get_internal_links(url, base_url)

    print(f"Found {len(links)} internal links")

    # Отправляем ссылки в очередь RabbitMQ
    send_to_queue(links, 'urls')


if __name__ == "__main__":
    asyncio.run(main())
