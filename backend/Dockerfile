FROM ubuntu:latest

WORKDIR /app

RUN apt update && \
    apt install -y bash \
                   build-essential \
                   git \
                   curl \
                   ca-certificates \
                   python3 \
                   python3-pip && \
    rm -rf /var/lib/apt/lists

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

COPY ./app/requirements.txt /app/requirements.txt

RUN python3 -m pip install pip --upgrade
RUN python3 -m pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./app /app

CMD ["uvicorn", "app.main:api", "--host", "0.0.0.0", "--reload", "--port", "3000"]