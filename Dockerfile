# Docker file to build container image based on 
# a miniconda image from the docker cloud
FROM python:3.7-slim AS builder
LABEL maintainer="Sebastian Arboleda <sebastian.a.arboleda@lions.enc.edu>" 
LABEL Name="connector Version=0.1.0"

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

COPY ./requirements /opt/connector/requirements
COPY ./connector /opt/connector

# ============ PRODUCTION ENV ============
FROM builder AS production

RUN python -m pip install -r /opt/connector/requirements/prod.txt
CMD /bin/bash -c "python -u connector.py"

# ============ TESTING ENV ============
FROM builder AS testing
COPY ./tests /opt/connector/tests
COPY ./test.sh /opt/connector

RUN python -m pip install -r /opt/connector/requirements/test.txt

WORKDIR /opt/connector
CMD ["./test.sh"]

# ============ DEVELOPMENT ENV ============
FROM builder AS development
COPY ./tests /opt/tests

RUN python -m pip install -r /opt/connector/requirements/dev.txt
CMD /bin/bash -c "python -u connector.py"