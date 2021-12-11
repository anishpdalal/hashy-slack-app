FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y bash \
                   build-essential \
                   git \
                   curl \
                   ca-certificates \
                   g++ \
                   make \
                   cmake \
                   unzip \
                   libcurl4-openssl-dev \
                   python3 \
                   python3-pip && \
    rm -rf /var/lib/apt/lists

RUN mkdir -p /function

COPY ./requirements.txt /function/requirements.txt
RUN python3 -m pip install pip --upgrade
RUN python3 -m pip install --no-cache-dir --upgrade -r /function/requirements.txt

COPY app.py /function
WORKDIR /function
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "app.handler" ]