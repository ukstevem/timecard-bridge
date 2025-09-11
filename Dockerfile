FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt requests python-dotenv
COPY bridge_mqtt_to_supabase.py /app/
ENV PYTHONUNBUFFERED=1
CMD ["python", "bridge_mqtt_to_supabase.py"]
