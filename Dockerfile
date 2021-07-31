FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get clean all
RUN apt-get update
RUN apt-get install -y python3-pip
RUN python --version
RUN pip --version
RUN pip install --upgrade pip
RUN mkdir /factory
WORKDIR /app

COPY requirements.txt requirements.txt
COPY src /app/src/
COPY setup.py setup.py

RUN pip install -r requirements.txt
RUN pip install .
