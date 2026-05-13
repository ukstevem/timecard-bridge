FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt requests python-dotenv
COPY bridge_mqtt_to_supabase.py /app/
RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1
ENV OUTBOX_PATH=/data/outbox.db
CMD ["python", "bridge_mqtt_to_supabase.py"]
