FROM ubuntu:20.04

RUN apt-get -y update
RUN apt-get -y install aria2
RUN apt-get -y install awscli
RUN apt-get -y install python3-pip
RUN pip3 install --no-cache-dir kb-python
RUN mkdir ~/.aws
RUN echo "[default]\nregion = us-east-1" > ~/.aws/config
RUN aria2c https://cf.10xgenomics.com/misc/bamtofastq-1.3.2
RUN chmod +x bamtofastq-1.3.2
RUN kb ref -d human -i kb_human.idx -g kbtg.txt
RUN aria2c https://cf.10xgenomics.com/misc/bamtofastq-1.3.2
COPY run.py .
