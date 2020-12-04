# image: ngeo/translate

#FROM intelpython/intelpython3_full
FROM spagnuolocarmine/docker-mpi

COPY . /app
WORKDIR /app

# RUN apt install libopenmpi-dev

RUN mkdir /opt/apps

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "-u", "driver.py"]