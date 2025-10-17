import streamlit as st
import pandas as pd
import psycopg2
from time import sleep

# إعدادات الاتصال بـ PostgreSQL (داخل شبكة Docker)
DB_HOST = 'postgres'
DB_NAME = 'gps_db'
DB_USER = 'user'
DB_PASSWORD = 'password'
TABLE_NAME = 'realtime_gps_data'

st.set_page_config(
    page_title="Realtime GPS Stream",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🛰️ تتبع بيانات GPS في الوقت الفعلي")
st.markdown("يتم تحديث هذه البيانات مباشرة من قاعدة بيانات PostgreSQL التي تغذيها Spark Streaming.")

# حاوية لوضع الجدول وخريطة البداية
col1, col2 = st.columns([1, 1])

def fetch_data():
    """يتصل بقاعدة البيانات ويجلب أحدث 50 صفاً."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        query = f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp_ms DESC LIMIT 50;"
        df = pd.read_sql(query, conn)
        conn.close()
        
        # تحويل الأعمدة إلى الأنواع الصحيحة
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        return df

    except Exception as e:
        # st.error(f"Failed to connect to PostgreSQL: {e}")
        return pd.DataFrame()

# حلقة التحديث المستمر
while True:
    data_df = fetch_data()
    
    with col1:
        st.header("أحدث 10 سجلات")
        if not data_df.empty:
            # عرض أول 10 سجلات في جدول
            st.dataframe(data_df.head(10), use_container_width=True)
        else:
            st.info("لا توجد بيانات متاحة بعد. هل Producer يعمل؟")

    with col2:
        st.header("توزيع البيانات على الخريطة")
        if not data_df.empty and not data_df[['latitude', 'longitude']].isnull().any().any():
            # عرض الخريطة
            st.map(data_df[['latitude', 'longitude']])
        else:
            st.warning("الخريطة في انتظار بيانات موقع صحيحة.")

    # تحديث كل 5 ثواني
    sleep(5)
