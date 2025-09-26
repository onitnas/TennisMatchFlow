FROM python:alpine3.21

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY ../data/tennis_match_events.json /app/tennis_match_events.json
COPY script.py /app/script.py
WORKDIR /app

CMD ["python", "script.py"]