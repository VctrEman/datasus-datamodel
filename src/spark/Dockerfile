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

# Install PySpark, findspark
RUN pip install -q findspark pyspark==${SPARK_VERSION}

# Create directories for JAR files and temporary downloads
ENV TMP="download_jars"
RUN mkdir -p /$TMP/jars
# List of JARs package dependencies to download, extracted from python submit downloads. Including it here will reduce one step when running the container
RUN curl -L -o /$TMP/jars/com.fasterxml.jackson.core_jackson-core-2.10.5.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.10.5/jackson-core-2.10.5.jar \
    && curl -L -o /$TMP/jars/com.google.code.findbugs_jsr305-3.0.2.jar https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar \
    && curl -L -o /$TMP/jars/com.google.errorprone_error_prone_annotations-2.2.0.jar https://repo1.maven.org/maven2/com/google/errorprone/error_prone_annotations/2.2.0/error_prone_annotations-2.2.0.jar \
    && curl -L -o /$TMP/jars/com.google.guava_failureaccess-1.0.jar https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0/failureaccess-1.0.jar \
    && curl -L -o /$TMP/jars/com.google.guava_guava-27.0-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar \
    && curl -L -o /$TMP/jars/com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar https://repo1.maven.org/maven2/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar \
    && curl -L -o /$TMP/jars/com.google.j2objc_j2objc-annotations-1.1.jar https://repo1.maven.org/maven2/com/google/j2objc/j2objc-annotations/1.1/j2objc-annotations-1.1.jar \
    && curl -L -o /$TMP/jars/com.microsoft.azure_azure-keyvault-core-1.0.0.jar https://repo1.maven.org/maven2/com/microsoft/azure/azure-keyvault-core/1.0.0/azure-keyvault-core-1.0.0.jar \
    && curl -L -o /$TMP/jars/com.microsoft.azure_azure-storage-7.0.1.jar https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar \
    && curl -L -o /$TMP/jars/commons-codec_commons-codec-1.11.jar https://repo1.maven.org/maven2/commons-codec/commons-codec/1.11/commons-codec-1.11.jar \
    && curl -L -o /$TMP/jars/commons-logging_commons-logging-1.1.3.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar \
    && curl -L -o /$TMP/jars/org.apache.hadoop.thirdparty_hadoop-shaded-guava-1.1.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar \
    && curl -L -o /$TMP/jars/org.apache.hadoop_hadoop-azure-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar \
    && curl -L -o /$TMP/jars/org.apache.httpcomponents_httpclient-4.5.13.jar https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar \
    && curl -L -o /$TMP/jars/org.apache.httpcomponents_httpcore-4.4.13.jar https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.13/httpcore-4.4.13.jar \
    && curl -L -o /$TMP/jars/org.checkerframework_checker-qual-2.5.2.jar https://repo1.maven.org/maven2/org/checkerframework/checker-qual/2.5.2/checker-qual-2.5.2.jar \
    && curl -L -o /$TMP/jars/org.codehaus.jackson_jackson-core-asl-1.9.13.jar https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar \
    && curl -L -o /$TMP/jars/org.codehaus.jackson_jackson-mapper-asl-1.9.13.jar https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar \
    && curl -L -o /$TMP/jars/org.codehaus.mojo_animal-sniffer-annotations-1.17.jar https://repo1.maven.org/maven2/org/codehaus/mojo/animal-sniffer-annotations/1.17/animal-sniffer-annotations-1.17.jar \
    && curl -L -o /$TMP/jars/org.eclipse.jetty_jetty-util-9.4.40.v20210413.jar https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.40.v20210413/jetty-util-9.4.40.v20210413.jar \
    && curl -L -o /$TMP/jars/org.eclipse.jetty_jetty-util-ajax-9.4.40.v20210413.jar https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.40.v20210413/jetty-util-ajax-9.4.40.v20210413.jar \
    && curl -L -o /$TMP/jars/org.slf4j_slf4j-api-1.7.30.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar \
    && curl -L -o /$TMP/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

# Move JARs to Spark jars directory
RUN mv /$TMP/jars/* $SPARK_HOME/jars/ 
#&& \
#    rm -rf /$TMP/jars


#ENV PATH=$PATH:/src
#ENV PYTHONPATH=/src

# caching directory is being set in the running code
RUN mkdir -p /home/_azbatch/pysus && chmod 777 /home/_azbatch/pysus

# Copy the application code to the container
ADD ./../silver/ /src
WORKDIR /src/

# Step 9: Define the entry point for the container to run the PySpark job
#ENTRYPOINT ["spark-submit", "/app/your_pyspark_script.py"]