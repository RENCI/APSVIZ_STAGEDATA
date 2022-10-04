##############
# Docker file for the creation of the stagedata script
#
# to create image: docker build -t stagedata:latest .
# to push image:
#       docker tag stagedata:latest renciorg/stagedata:latest
#       docker push renciorg/stagedata:latest
##############
FROM python:3.9-slim

# set the logging level
ENV LOG_LEVEL=10

# get some credit
LABEL maintainer="jtilson@renci.org"

# create a new non-root user and switch to it
RUN useradd --create-home -u 1000 nru
USER nru

# Create the directory for the code and cd to it
WORKDIR /repo/APSVIZ_STAGEDATA

# Copy in just the requirements first for caching purposes
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy in the rest of the code
COPY common common
COPY ./*.py ./

##########
# at this point the container is ready to accept the launch command.
# see stagedata_job.yaml for the job that launches this container.
# example command lines:
# python stage_data.py --inputURL http://tds.renci.org:80/thredds/fileServer/2022/al09/23/hsofs/hatteras.renci.org/hsofs-al09-bob/nhcOfcl --isHurricane 09 --outputDir /data/4278-23-nhcOfcl/input/
# python finalize_data.py --inputDir /data/4278-23-nhcOfcl/final --outputDir /data/final --tarMeta 4278-23-nhcOfcl
##########
