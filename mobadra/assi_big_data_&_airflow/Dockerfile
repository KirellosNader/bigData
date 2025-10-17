FROM python:3.10-slim

# إعداد دليل العمل
WORKDIR /app

# نسخ ملف متطلبات Python
COPY requirements.txt .

# تثبيت متطلبات Python
RUN pip install --no-cache-dir -r requirements.txt

# إعداد متغير البيئة لتشغيل Streamlit
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

CMD ["streamlit", "run", "app.py"]
