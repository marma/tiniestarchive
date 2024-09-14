FROM ubuntu:22.04

ADD ./app /app
ADD ./tiniestarchive /app/tiniestarchive

COPY requirements.txt /

RUN apt update && apt install -y python3 python3-venv python3-pip
RUN python3 -m venv /venv && . /venv/bin/activate && pip3 install -r /requirements.txt

WORKDIR /app

CMD . /venv/bin/activate && python3 -m uvicorn --reload app:app --host 0.0.0.0 --port 80

EXPOSE 80

