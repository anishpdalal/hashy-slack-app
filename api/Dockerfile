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

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

RUN mkdir -p /function

COPY ./requirements.txt /function/requirements.txt
RUN python3 -m pip install pip --upgrade
RUN python3 -m pip install --no-cache-dir --upgrade -r /function/requirements.txt

COPY app.py /function
RUN mkdir -p /mnt/data
RUN mkdir -p /mnt/cross_encoder
COPY data /mnt/data
COPY cross_encoder /mnt/cross_encoder
WORKDIR /function
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "app.handler" ]