FROM python:3.11.10-slim-bullseye

ENV SPARK_VERSION=3.3.4 \
    HADOOP_VERSION=3 \
    JAVA_VERSION=11 \
    SPARK_HOME=/opt/spark \
    PACKAGES="org.apache.hadoop:hadoop-azure:3.3.1"

# Install Java 11 (JRE)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Cache Spark package dependencies
RUN $SPARK_HOME/bin/spark-submit --packages $PACKAGES \
    --dry-run /dev/null || true

# Install PySpark, findspark
RUN pip install -q findspark pyspark==${SPARK_VERSION}

ENV PATH=$PATH:/src
ENV PYTHONPATH=/src

RUN curl -L -o azcopy.tar.gz https://aka.ms/downloadazcopy-v10-linux && \
    tar --strip-components=1 -C /usr/local/bin --no-same-owner --exclude="*.txt" -xzvf azcopy.tar.gz && \
    chmod 755 /usr/local/bin/azcopy && \
    rm azcopy.tar.gz
    
# Create writeable directory for azcopy, declare writeable directory for azcopy
ENV AZCOPY_LOG_LOCATION=/home/_azbatch/azcopy/azcopy_logs
ENV AZCOPY_JOB_PLAN_LOCATION=/home/_azbatch/azcopy/azcopy_job_plan

RUN mkdir -p $AZCOPY_LOG_LOCATION && \
  mkdir -p $AZCOPY_JOB_PLAN_LOCATION && \
  chmod 777 $AZCOPY_LOG_LOCATION $AZCOPY_JOB_PLAN_LOCATION

# caching directory is being set in the running code
RUN mkdir -p /home/_azbatch/pysus && chmod 777 /home/_azbatch/pysus

# Copy the application code to the container
ADD ./../silver/ /src
WORKDIR /src/

# Step 9: Define the entry point for the container to run the PySpark job
#ENTRYPOINT ["spark-submit", "/app/your_pyspark_script.py"]