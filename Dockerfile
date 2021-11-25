# syntax=docker/dockerfile:1
FROM python:3

WORKDIR /usr/src/app

RUN pip install --no-cache-dir flask requests

COPY . .

CMD [ "python", "./shardedkvs.py"]