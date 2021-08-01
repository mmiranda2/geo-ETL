FROM osgeo/gdal:ubuntu-full-latest

ENV ETL_DIR=/factory
RUN apt-get clean all
RUN apt-get update
RUN apt-get install -y python3-pip
RUN python --version
RUN pip --version
RUN pip install --upgrade pip
RUN mkdir $ETL_DIR
WORKDIR /app

COPY requirements.txt requirements.txt
COPY src /app/src/
COPY setup.py setup.py

RUN pip install -r requirements.txt
RUN pip install .
