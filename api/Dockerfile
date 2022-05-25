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
RUN mkdir -p /mnt/bi_encoder
RUN mkdir -p /mnt/cross_encoder
RUN mkdir -p /mnt/tokenizer
RUN mkdir -p /mnt/intention_model
COPY bi_encoder /mnt/bi_encoder
COPY cross_encoder /mnt/cross_encoder
COPY intention_model /mnt/intention_model
RUN python3 -c "from transformers import AutoTokenizer; tokenizer = AutoTokenizer.from_pretrained('distilbert-base-uncased'); tokenizer.save_pretrained('/mnt/tokenizer');"
WORKDIR /function
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "app.handler" ]