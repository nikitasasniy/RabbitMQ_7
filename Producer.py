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
                if href.startswith('http') or href.startswith('https'):
                    if base_url in href:
                        links.append(href)
                else:
                    links.append(urljoin(base_url, href))
            return links


# Функция для подключения к RabbitMQ и отправки ссылок
def send_to_queue(links, queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST', 'localhost')))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

    for link in links:
        channel.basic_publish(exchange='', routing_key=queue_name, body=link)
        print(f"Sent: {link}")

    connection.close()


async def main():
    if len(sys.argv) < 2:
        print("Usage: producer.py <URL>")
        return

    url = sys.argv[1]
    base_url = urlparse(url).netloc

    print(f"Processing {url}")

    links = await get_internal_links(url, base_url)

    print(f"Found {len(links)} internal links")

    send_to_queue(links, 'urls')


if __name__ == "__main__":
    asyncio.run(main())
