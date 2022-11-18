#Defines the image of which the container 
#is going to be based 
FROM python:3.8

ARG NAME
#The containers’ copies in its storage the 
#requirements.txt file from the local machine
#This file is filled with all necessary libraries
#that we need to install before the container’s launch

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

RUN wget https://dlcdn.apache.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz && \
    tar xvf spark-* && \
    mv spark-3.2.2-bin-hadoop3.2 /opt/spark && \
    rm spark-*
    
COPY ./infrastructures/app/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY ./src/services/app/packages /src/app/packages
COPY ./src/services/app/app.py /src/app/app.py
COPY ./src/services/app/__init__.py /src/app/__init__.py
COPY ./src/services/app/uploaded_files /src/app/uploaded_files

WORKDIR /src/app/

#It defines the port number the container should expose

#Finally, in order our container to run the python 
#code we do it with final command
CMD [ "python3", "./app.py" ]

