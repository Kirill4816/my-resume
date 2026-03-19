import json
import time
import os
import sys
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Настройки
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
RAW_TOPIC = os.getenv('RAW_TOPIC', 'raw_events')
ENRICHED_TOPIC = os.getenv('ENRICHED_TOPIC', 'enriched_events')
ALERTS_TOPIC = os.getenv('ALERTS_TOPIC', 'alerts')

print("=" * 60)
print("⚙️ KAFKA PROCESSOR - ЗАПУСК")
print(f"📍 Брокер: {KAFKA_BROKER}")
print(f"📥 Вход: {RAW_TOPIC}")
print(f"📤 Выход: {ENRICHED_TOPIC}, {ALERTS_TOPIC}")
print("=" * 60)
sys.stdout.flush()

# Детектор аномалий
login_attempts = defaultdict(list)

def process_event(raw_event):
    """Обогащает и анализирует событие."""
    enriched_event = raw_event.copy()
    enriched_event['processed_at'] = datetime.now().isoformat()
    enriched_event['processor'] = 'docker'

    alert = None
    if raw_event['action'] == 'login':
        user = raw_event['user_id']
        now = time.time()
        login_attempts[user].append(now)
        login_attempts[user] = [t for t in login_attempts[user] if now - t < 60]
        
        if len(login_attempts[user]) > 3:
            alert = {
                'type': 'TOO_MANY_LOGINS',
                'user_id': user,
                'count': len(login_attempts[user]),
                'timestamp': datetime.now().isoformat(),
                'message': f'User {user} - {len(login_attempts[user])} logins in 1 minute!'
            }

    return enriched_event, alert

def run_processor():
    print("🔄 Попытка подключиться к Kafka...")
    print(f"   Брокер: {KAFKA_BROKER}")
    print(f"   Топик: {RAW_TOPIC}")
    sys.stdout.flush()
    
    try:
        print("📡 Создание KafkaConsumer...")
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            group_id='processor-debug-' + str(int(time.time())),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(2, 0, 2),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=1,  # По одному сообщению
            connections_max_idle_ms=540000,
            request_timeout_ms=40000,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        print("✅ Consumer создан успешно!")
        sys.stdout.flush()

        print("📡 Создание KafkaProducer...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2),
            acks='all'
        )
        print("✅ Producer создан успешно!")
        sys.stdout.flush()

        print("✅ Подключено к Kafka!")
        print("⏳ Ожидание сообщений... (это может занять время)")
        print("-" * 60)
        sys.stdout.flush()

        message_count = 0
        poll_count = 0
        
        while True:
            poll_count += 1
            print(f"🔍 Poll #{poll_count} - ожидание сообщений (timeout=5000ms)...", flush=True)
            
            records = consumer.poll(timeout_ms=5000, max_records=1)
            
            if not records:
                print(f"⏱️ Poll #{poll_count}: нет сообщений (пусто)", flush=True)
                continue
            
            print(f"✨ Poll #{poll_count}: получено {len(records)} partitions", flush=True)
            
            for topic_partition, messages in records.items():
                print(f"📍 Partition: {topic_partition}, Messages: {len(messages)}", flush=True)
                
                for message in messages:
                    raw_event = message.value
                    message_count += 1
                    print(f"📥 [{message_count}] Получено: User {raw_event['user_id']} - {raw_event['action']}", flush=True)

                    enriched_event, alert = process_event(raw_event)

                    producer.send(ENRICHED_TOPIC, value=enriched_event)
                    producer.flush()
                    print(f" 📤 -> enriched_events", flush=True)

                    if alert:
                        producer.send(ALERTS_TOPIC, value=alert)
                        producer.flush()
                        print(f" 🚨 ALERT -> {alert['message']}", flush=True)

    except KeyboardInterrupt:
        print("\n🛑 Processor остановлен пользователем")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ ОШИБКА: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()

if __name__ == '__main__':
    run_processor()
