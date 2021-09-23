# syntax=docker/dockerfile:1
# TODO: Install Hadoop
FROM amazoncorretto:8

ENV SBT_VERSION 1.5.5

RUN yum -y install unzip
RUN curl -L -o sbt-$SBT_VERSION.zip https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-$SBT_VERSION.zip
RUN unzip sbt-$SBT_VERSION.zip -d ops

WORKDIR /SparkersAssignment

ADD src /SparkersAssignment/src
ADD target /SparkersAssignment/target
ADD project /SparkersAssignment/project
ADD build.sbt /SparkersAssignment

CMD /ops/sbt/bin/sbt run