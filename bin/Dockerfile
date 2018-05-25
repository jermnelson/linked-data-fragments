#Dockerfile for Linked Data Fragments Base
FROM python:3.5.1
MAINTAINER Jeremy Nelson <jermnelson@gmail.com>

# Set environmental variables
ENV LDFS_HOME /opt/ldfs

# Update Ubuntu and install Python 3 setuptools, git and other
# packages
RUN apt-get update && apt-get install -y && \
  apt-get install -y python3-setuptools &&\
  apt-get install -y git &&\
  apt-get install -y python3-pip


# Retrieve latest development branch of Linked Data Fragments project on 
# github.com
RUN git clone https://github.com/jermnelson/linked-data-fragments.git $LDFS_HOME \
    && cd $LDFS_HOME \
    && git checkout -b development \
    && git pull origin development \
    && pip3 install -r requirements.txt \
    && touch __init__.py 
   
WORKDIR $LDFS_HOME
CMD ["nohup", "python", "server.py", "&"]
