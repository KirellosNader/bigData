import time
import json
from faker import Faker
from kafka import KafkaProducer

# إعداد Faker
fake = Faker()

# تأكد من استخدام المنفذ الخارجي الصحيح: 29092
# ملاحظة: يجب أن يكون هذا 29092 بدلاً من 9092
KAFKA_BROKER = 'localhost:29092' 
KAFKA_TOPIC = 'gps_topic'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    # طريقة تحويل البيانات إلى بايت قبل الإرسال
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer starting, sending data to topic: {KAFKA_TOPIC}")

def generate_gps_data():
    """ينشئ مجموعة من بيانات GPS الوهمية، مع التأكد من التحويل إلى float."""
    data = {
        'device_id': fake.uuid4(),
        'latitude': float(fake.latitude()),  
        'longitude': float(fake.longitude()),
        'speed': fake.random_int(min=0, max=120),
        'timestamp': int(time.time() * 1000)
    }
    return data

try:
    while True:
        gps_data = generate_gps_data()
        
        # إرسال البيانات إلى Kafka
        producer.send(KAFKA_TOPIC, value=gps_data)
        
        print(f"Sent: {gps_data}")
        
        # انتظار لمدة ثانية قبل إرسال البيانات التالية
        time.sleep(1) 

except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.close()
    
except Exception as e:
    print(f"An error occurred: {e}")
    producer.close()
