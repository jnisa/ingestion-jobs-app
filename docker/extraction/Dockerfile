FROM ubuntu:latest

# Set the working directory in the image
RUN mkdir /usajobs
RUN mkdir /usajobs/ingestion
WORKDIR /usajobs

# Add crontab file in the cron directory
# ATTENTION if you want to pull resource to cloud use the following line instead
# and make sure you copy the setup, the requirements and the ingestion folder
# COPY crontab /etc/cron.d/tasman-cron
COPY ./docker/extraction/crontab /etc/cron.d/tasman-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/tasman-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Copy the requirements.txt and setup.py to the container
COPY setup.py .
COPY requirements.txt .

# Copy the contents of the current directory to the image
COPY ingestion ./ingestion/

# Install required packages
RUN apt-get update
RUN apt-get -y install cron
RUN apt-get -y install libpq-dev
RUN apt-get -y install python3-pip
RUN pip3 install -r requirements.txt
RUN python3 setup.py install
RUN pip3 install pytest
RUN pip3 install pytest-mock

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log