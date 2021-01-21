##############
# Docker file for the creation of the stagedata script
#
# to create image: docker build -t stagedata:latest .
# to push image:
#       docker tag stagedata:latest renciorg/stagedata:latest
#       docker push renciorg/stagedata:latest
##############
FROM continuumio/miniconda3

# get some credit
LABEL maintainer="jtilson@renci.org"

# make sure the container is up-to-date
RUN apt-get update

# Start off getting Python 3.7.3
RUN conda install  -y python=3.7.3

# update conda and set the download channel
RUN conda update conda && \
    conda config --add channels conda-forge

# tell conda what the shell is
RUN conda init bash

# make a directory for the repos and go there
RUN mkdir /repo
WORKDIR /repo

# get the repos
RUN git clone https://github.com/RENCI/APSVIZ_STAGEDATA.git
               
# move to the install directory
WORKDIR APSVIZ_STAGEDATA/INSTALL

# install the pip requirements
#RUN pip install -r requirements.txt
RUN conda install --yes --file requirements.txt

# change to the pipelines directory                        
WORKDIR /repo/APSVIZ_STAGEDATA
                                   
# make a temporary directory for the output.
# this should eventually point a common PV
RUN mkdir temp

# set the location of the output directory
ENV RUNTIMEDIR=/data

##########
# at this point the container is ready to accept the launch command.
# see stagedata_job.yaml for the job that launches this container.
##########
