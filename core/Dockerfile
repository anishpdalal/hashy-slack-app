FROM ubuntu:latest

ENV DEBIAN_FRONTEND=nonintercative

RUN apt update && \
    apt install -y bash \
                   build-essential \
                   git \
                   curl \
                   ca-certificates \
                   python3 \
                   python3-pip && \
    rm -rf /var/lib/apt/lists

RUN apt-get update
RUN apt-get install -y python3.8-venv

RUN mkdir -p /core/core

COPY setup.py /core
COPY core /core/core

RUN python3 -m pip install pip --upgrade build
RUN python3 -m pip install wheel
RUN python3 -m build /core