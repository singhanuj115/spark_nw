FROM python:3.9.0b4-alpine3.12

COPY spark_nw.py /bin/spark_nw.py
COPY root /var/spool/cron/crontabs/root


RUN apk add --no-cache --update \
    python3 python3-dev gcc \
    gfortran musl-dev g++ \
    libffi-dev openssl-dev \
    libxml2 libxml2-dev \
    libxslt libxslt-dev \
    libjpeg-turbo-dev zlib-dev

RUN pip install --upgrade pip
ADD requirements.txt .
RUN pip install -r requirements.txt


RUN chmod +x /bin/spark_nw.py
CMD crond -l 2 -f