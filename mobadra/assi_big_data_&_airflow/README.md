🛰️ مشروع نظام تتبع المركبات اللحظي باستخدام Spark Streaming و Kafka

هذا المشروع يمثل خط أنابيب (Pipeline) كامل للبيانات الضخمة (Big Data) يهدف إلى جمع، معالجة، وتخزين بيانات تتبع المركبات (GPS) في الوقت الفعلي، وعرضها على لوحة تحكم تفاعلية.

🚀 المكونات الأساسية

Kafka: نظام المراسلة الموزع (Broker) لنقل بيانات GPS لحظيًا (اسم الخدمة: kafka).

Spark Streaming: محرك معالجة البيانات الضخمة، يقوم بقراءة البيانات من Kafka وتحويلها (خدمات: spark-master و spark-worker).

PostgreSQL: قاعدة بيانات علائقية لتخزين البيانات المعالجة من Spark (اسم الخدمة: postgres).

Python Producer: برنامج (Script) يقوم بتوليد وإرسال بيانات GPS الوهمية إلى Kafka.

Streamlit: واجهة أمامية (Frontend) تفاعلية لعرض البيانات في الوقت الفعلي على خريطة (اسم الخدمة: streamlit).

Docker Compose: أداة لتشغيل وإدارة جميع الخدمات المذكورة أعلاه في بيئة موحدة ومعزولة.

📁 هيكل المشروع

للتشغيل الصحيح، يجب أن يحتوي المجلد الرئيسي (الذي يوجد فيه ملف docker-compose.yml) على الملفات التالية:

docker-compose.yml: تعريف وإعداد جميع الخدمات.

spark_streaming.py: كود Spark Streaming لمعالجة البيانات وتخزينها في Postgres.

gps_producer.py: كود Python لتوليد بيانات GPS وإرسالها إلى Kafka.

app.py: كود Streamlit لعرض البيانات من Postgres.

README.md: ملف التوثيق هذا.

🛠️ دليل التشغيل خطوة بخطوة

لضمان عمل المشروع بالكامل، اتبع الخطوات التالية بالترتيب:

1. إعداد البيئة (Docker Compose)

قم بتشغيل جميع الخدمات باستخدام Docker Compose.

docker compose up -d


2. تشغيل مهمة Spark Streaming

بعد تأكدك من أن خدمات Spark و Kafka جاهزة، قم بتشغيل مهمة Spark Streaming.

docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --conf spark.driver.extraJavaOptions=-Dorg.apache.spark.launcher.conf=/opt/spark/conf/spark-defaults.conf \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /app/spark_streaming.py


ملاحظة: ابقِ هذه النافذة مفتوحة لمراقبة سجلات Spark.

3. تشغيل الـ Producer (توليد البيانات)

افتح نافذة طرفية (Terminal) جديدة، وقم بتشغيل Producer لإرسال البيانات العشوائية إلى Kafka.

docker exec -d spark-master python /app/gps_producer.py


4. عرض النتائج على Streamlit

بعد تشغيل الـ Producer وبدء Spark في تخزين البيانات، افتح المتصفح وتوجه إلى:

http://localhost:8501

5. إيقاف المشروع

لإيقاف جميع الخدمات وإزالة الحاويات:

docker compose down
