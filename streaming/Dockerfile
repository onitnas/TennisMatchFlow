FROM bellsoft/liberica-openjdk-alpine:25

RUN apk add openssl curl

COPY run.sh /home/run.sh
COPY connectors /connectors

CMD ["sh", "/home/run.sh"]
