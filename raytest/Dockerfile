# image: ngeo/translate

FROM python:3.7.2

COPY . /app
WORKDIR /app

RUN pip install ray

ENTRYPOINT ["python", "driver.py"]