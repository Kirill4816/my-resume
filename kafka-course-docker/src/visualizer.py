import json
import time
import os
from colorama import init, Fore, Back, Style
from kafka import KafkaConsumer

# Инициализация colorama
init(autoreset=True)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
ENRICHED_TOPIC = os.getenv('ENRICHED_TOPIC', 'enriched_events')
ALERTS_TOPIC = os.getenv('ALERTS_TOPIC', 'alerts')

def run_visualizer():
    # Подписываемся на два топика
    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        ALERTS_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        group_id='visualizer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 0, 2),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_records=100
    )

    print(Fore.CYAN + "=" * 60)
    print(Fore.YELLOW + "📊 KAFKA VISUALIZER (Docker)")
    print(Fore.CYAN + "=" * 60)
    print(Fore.WHITE + f"📍 Брокер: {KAFKA_BROKER}")
    print(Fore.WHITE + f"👁️ Мониторинг топиков: {ENRICHED_TOPIC}, {ALERTS_TOPIC}")
    print(Fore.CYAN + "=" * 60 + "\n")

    # Цвета для действий
    action_colors = {
        'login': Fore.GREEN + Style.BRIGHT,
        'logout': Fore.CYAN + Style.BRIGHT,
        'view_page': Fore.BLUE,
        'click_button': Fore.MAGENTA,
        'add_to_cart': Fore.YELLOW,
        'purchase': Fore.YELLOW + Back.BLACK + Style.BRIGHT,
        'error': Fore.RED + Style.BRIGHT
    }

    try:
        for message in consumer:
            data = message.value
            topic = message.topic

            if topic == ENRICHED_TOPIC:
                action = data.get('action', 'unknown')
                color = action_colors.get(action, Fore.WHITE)
                print(f"{color}📈 [EVENT] User {data['user_id']:3d} | {action.upper():12} | "
                      f"Page: {str(data.get('page', 'N/A'))[:20]:20} | "
                      f"Time: {data.get('processed_at', 'N/A')[11:19]}")

            elif topic == ALERTS_TOPIC:
                print(Fore.RED + Style.BRIGHT + "═" * 60)
                print(Fore.RED + Style.BRIGHT + f"🚨🚨 [SECURITY ALERT] {data['type']}")
                print(Fore.RED + Style.BRIGHT + f" 👤 User: {data['user_id']}")
                print(Fore.RED + Style.BRIGHT + f" 📊 Count: {data['count']} attempts")
                print(Fore.RED + Style.BRIGHT + f" 📝 {data['message']}")
                print(Fore.RED + Style.BRIGHT + "═" * 60)
                print(Style.RESET_ALL)

    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n👋 Visualizer остановлен.")
    except Exception as e:
        print(Fore.RED + f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_visualizer()
