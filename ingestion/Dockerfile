FROM python:alpine3.21

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY ./data/tennis_match_events.json /app/tennis_match_events.json
COPY tennis_producer.py /app/tennis_producer.py
WORKDIR /app

CMD ["python", "tennis_producer.py"]