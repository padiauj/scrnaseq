FROM ubuntu:20.04

RUN apt-get -y update
RUN apt-get -y install aria2
RUN apt-get -y install awscli
RUN apt-get -y install python3-pip
RUN pip3 install --no-cache-dir kb-python
RUN mkdir ~/.aws
RUN echo "[default]\nregion = us-east-1" > ~/.aws/config
COPY run.py .
