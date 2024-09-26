FROM python:3.10.14-slim-bullseye

# Install OpenJDK 8, and monitoring tooling
RUN \
  apt-get update && \
  apt-get install -y gcc curl && \
  rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade setuptools
COPY requirements.txt /
RUN pip install -r /requirements.txt

ENV PATH=$PATH:/src
ENV PYTHONPATH=/src

RUN curl -L -o azcopy.tar.gz https://aka.ms/downloadazcopy-v10-linux && \
    tar --strip-components=1 -C /usr/local/bin --no-same-owner --exclude="*.txt" -xzvf azcopy.tar.gz && \
    chmod 755 /usr/local/bin/azcopy && \
    rm azcopy.tar.gz

# pysus caching directory is being set in the running code
RUN mkdir -p /home/_azbatch/pysus && chmod 777 /home/_azbatch/pysus

ADD ./../landing/ /src
WORKDIR /src/


