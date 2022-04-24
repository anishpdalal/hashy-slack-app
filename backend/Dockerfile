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

COPY ./app/requirements.txt /app/requirements.txt
COPY ./dist/core-0.1.0-py3-none-any.whl /app/core-0.1.0-py3-none-any.whl
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install wheel
RUN python3 -m pip install --no-cache-dir --upgrade -r /app/requirements.txt
RUN python3 -m pip install --no-cache-dir /app/core-0.1.0-py3-none-any.whl

COPY ./app /app
WORKDIR /app

RUN useradd -m myuser
USER myuser

CMD ["uvicorn", "app.main:api", "--host", "0.0.0.0", "--reload", "--port", "3000"]