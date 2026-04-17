FROM python:3.10.6-slim-bullseye
RUN apt-get update
RUN apt-get install -y git ssh
RUN mkdir Hub
COPY  ./src/hub_federate/ ./Hub
COPY ./requirements.txt ./Hub/
WORKDIR ./Hub
RUN pip install -r requirements.txt
EXPOSE 5903/tcp
CMD ["python", "server.py"]
