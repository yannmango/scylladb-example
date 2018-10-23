From java:8
RUN apt-get update
RUN apt-get install -y maven
WORKDIR /opt/code
COPY . /opt/code/
ADD pom.xml /opt/code/
RUN ["mvn", "clean","compile","assembly:single"]
WORKDIR /opt/code/target
CMD tail -f /dev/null





