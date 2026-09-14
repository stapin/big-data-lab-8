FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre wget && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY ./workspace/requirements.txt /workspace/

RUN pip install --no-cache-dir -r requirements.txt

COPY ./workspace /workspace/