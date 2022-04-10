FROM alpine:3.14.6
RUN apk add python3-dev postgresql-dev py3-pip gcc musl-dev
COPY dist /tmp/dist
RUN pip3 install /tmp/dist/*.tar.gz
