import os
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
CDC_TOPIC=os.getenv("CDC_TOPIC","cdc.public.customers")
POSTGRES_HOST=os.getenv("POSTGRES_HOST","localhost")
POSTGRES_PORT=int(os.getenv("POSTGRES_PORT","5432"))
POSTGRES_DB=os.getenv("POSTGRES_DB","commerce")
POSTGRES_USER=os.getenv("POSTGRES_USER","cdc_user")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD","cdc_password")
