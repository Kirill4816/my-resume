import json
import time
import random
import os
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Настройки из переменных окружения
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
RAW_TOPIC = os.getenv('RAW_TOPIC', 'raw_events')

fake = Faker()

def create_event():
    """Генерирует случайное событие пользователя."""
    user_id = random.randint(1, 100)
    actions = ['login', 'logout', 'view_page', 'click_button', 'add_to_cart', 'purchase', 'error']
    action = random.choice(actions)
    event = {
        'user_id': user_id,
        'action': action,
        'page': f'/{fake.uri_page()}' if action in ['view_page', 'click_button'] else None,
        'timestamp': datetime.utcnow().isoformat(),
        'user_agent': fake.user_agent(),
        'ip_address': fake.ipv4(),
        'container': 'docker'
    }
    return event

def run_producer():
    # Создаем Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 0, 2)
    )

    print(f"🚀 Producer запущен в Docker!")
    print(f"📡 Подключение к: {KAFKA_BROKER}")
    print(f"🎯 Топик: {RAW_TOPIC}")
    print("=" * 50)

    try:
        message_count = 0
        while True:
            event = create_event()
            message_count += 1
            
            # Отправляем событие
            producer.send(RAW_TOPIC, value=event)
            
            # ✅ ИСПРАВЛЕНО: Выводим КАЖДОЕ событие (не только каждое 10-е)
            print(f"📤 [{message_count:4d}] User {event['user_id']:3d}: {event['action'].upper():12} | Page: {str(event['page'])[:20]:20}")
            
            # Flush каждые 5 событий для оптимизации
            if message_count % 5 == 0:
                producer.flush()
            
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\n🛑 Producer остановлен.")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.flush()
        producer.close()

if __name__ == '__main__':
    run_producer()
