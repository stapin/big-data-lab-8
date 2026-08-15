FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.0 python-dotenv numpy

WORKDIR /workspace

COPY ./workspace /workspace/