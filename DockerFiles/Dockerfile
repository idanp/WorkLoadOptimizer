########### First build stage - get the private keys fo git cloning and aws creds  #################
# Clone the most updated BDPTestFramework from git and

FROM python:3 as firstStage

# Install git

RUN apt-get update
RUN apt-get install -y git

# add credentials on build
RUN mkdir /root/.ssh/
COPY ../ssh_key /root/.ssh/
#RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa


# make sure your domain is accepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts
RUN chmod 700 /root/.ssh/ssh_key
RUN eval "$(ssh-agent -s)" && ssh-add /root/.ssh/ssh_key && git clone git@github.com:idanp/OWL.git


######### clean docker image with no SSH keys ############################
FROM python:3

# copy the repository form the previous image
COPY --from=firstStage /OWL /OWL

RUN pip install boto3

RUN pip install pandas

RUN pip install pyarrow

RUN pip install fastparquet

RUN pip install s3fs

WORKDIR /OWL

CMD python ./ProducerManager.py --logLevel INFO --publisher <Message publisher> --producingMode WU --logstdout true

