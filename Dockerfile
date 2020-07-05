# Docker file to build container image based on 
# a miniconda image from the docker cloud
FROM python:3.7-slim AS builder
LABEL maintainer="Sebastian Arboleda <sebastian.a.arboleda@lions.enc.edu>" 
LABEL Name="connector Version=0.1.0"

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

COPY ./requirements /requirements
COPY ./connector /

# ============ DEVELOPMENT ENV ============
FROM builder AS development
COPY ./tests /tests

RUN python -m pip install -r /requirements/dev.txt
CMD /bin/bash -c "python -u connector.py"

# ============ TESTING ENV ============
FROM builder AS testing
COPY ./tests /tests

RUN python -m pip install -r /requirements/test.txt
CMD /bin/bash -c "pytest -v"

# ============ PRODUCTION ENV ============
FROM builder AS production

RUN python -m pip install -r /requirements/prod.txt
CMD /bin/bash -c "python -u connector.py"
