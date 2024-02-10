# Use an official Ubuntu image as a parent image
#FROM ubuntu:20.04
FROM python:3.8.10
# Set environment variables to avoid time zone prompt
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Copy requirement file in working dir
COPY requirements.txt /RecSys_fp/requirements.txt 
# Set the working directory in the container
WORKDIR /RecSys_fp

# Install dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless wget python3.12 python3-pip

# Set up Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz && \
    tar -xzf hadoop-3.3.1.tar.gz && \
    mv hadoop-3.3.1 /usr/local/hadoop && \
    rm hadoop-3.3.1.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
	
# Download and install Spark 3.5.0
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /usr/local/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download MySQL Connector/J separately
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.28.tar.gz && \
    tar -xzvf mysql-connector-java-8.0.28.tar.gz && \
    mv mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar /usr/local/spark/jars/mysql-connector-java-8.0.28.jar && \
    rm -rf mysql-connector-java-8.0.28/ mysql-connector-java-8.0.28.tar.gz

# Upgrade pip and install findspark pyspark and pandas
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /RecSys_fp

# Command to run your Python application
CMD ["python3", "/RecSys_fp/main.py"]
