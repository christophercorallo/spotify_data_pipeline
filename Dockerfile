FROM python:3.11-slim 
WORKDIR /usr/local/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD write_data_to_postgres.py .

RUN useradd app
USER app

CMD [“python”, “./write_data_to_postgres.py”] 