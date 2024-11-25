import os
import pika
import asyncio
import aiohttp
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
                if href.startswith('http') or href.startswith('https'):
                    if base_url in href:
                        links.append(href)
                else:
                    links.append(urljoin(base_url, href))
            return links


# Функция для обработки сообщений из очереди
async def process_message(ch, method, properties, body):
    url = body.decode()
    print(f"Processing {url}")

    base_url = urlparse(url).netloc
    links = await get_internal_links(url, base_url)

    print(f"Found {len(links)} internal links in {url}")

    for link in links:
        print(f"Found link: {link}")
        ch.basic_publish(exchange='', routing_key='urls', body=link)

    ch.basic_ack(delivery_tag=method.delivery_tag)


# Асинхронная функция для подключения и обработки очереди RabbitMQ
async def consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST', 'localhost')))
    channel = connection.channel()
    channel.queue_declare(queue='urls')

    def callback(ch, method, properties, body):
        loop = asyncio.get_event_loop()
        loop.create_task(process_message(ch, method, properties, body))

    channel.basic_consume(queue='urls', on_message_callback=callback, auto_ack=False)

    print('Waiting for messages...')
    channel.start_consuming()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume())
