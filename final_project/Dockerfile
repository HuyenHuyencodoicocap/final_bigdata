FROM python:3.9-slim

WORKDIR /app

COPY src/Postgres/data_generator.py .
RUN pip install psycopg2-binary

CMD ["python", "data_generator.py"]