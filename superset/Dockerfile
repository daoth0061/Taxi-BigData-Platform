FROM apache/superset

USER root

ENV SUPERSET_SECRET_KEY=secret

RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN uv pip install \
    mysqlclient \
    pyhive \
    thrift \
    thrift_sasl \
    sasl
    
USER superset