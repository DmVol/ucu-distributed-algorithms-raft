FROM python:3.8-slim-buster
COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip install -r requirements.txt
COPY . /opt/app/
ENV PYTHONUNBUFFERED 1
EXPOSE 50051-50053