##############
# Docker file for the creation of the stagedata script
#
# to create image: docker build -t stagedata:latest .
# to push image:
#       docker tag stagedata:latest renciorg/stagedata:latest
#       docker push renciorg/stagedata:latest
##############

FROM python:3.8.5

# get some credit
LABEL maintainer="jtilson@renci.org"

# make a directory for the repos and go there
RUN mkdir /repo
WORKDIR /repo

# get the repos
RUN git clone https://github.com/RENCI/APSVIZ_STAGEDATA.git

# move to the install directory
WORKDIR /repo/APSVIZ_STAGEDATA

# install the pip requirements
RUN pip install -r requirements_minimal.txt

##########
# at this point the container is ready to accept the launch command.
# see stagedata_job.yaml for the job that launches this container.
##########
