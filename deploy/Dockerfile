##############
# Docker file for the creation of the stagedata script
#
# to create image: docker build -t stagedata:latest .
# to push image:
#       docker tag stagedata:latest renciorg/stagedata:latest
#       docker push renciorg/stagedata:latest
##############

FROM python:3.8.5

# set the logging level
ENV LOG_LEVEL=10

# get some credit
LABEL maintainer="jtilson@renci.org"

# create a non-root user with a home directory
RUN useradd -m -d /home/nru -u 1000 nru

# switch to the new non-root user
USER nru

# make a directory for the repos and go there
RUN mkdir /home/nru/repo
WORKDIR /home/nru/repo

# get the repos
RUN git clone https://github.com/RENCI/APSVIZ_STAGEDATA.git

# move to the install directory
WORKDIR /home/nru/repo/APSVIZ_STAGEDATA

# install the pip requirements
RUN pip install -r requirements.txt

RUN chmod 777 /home/nru

##########
# at this point the container is ready to accept the launch command.
# see stagedata_job.yaml for the job that launches this container.
##########
