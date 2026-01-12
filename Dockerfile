FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install Python 3.8 to match Spark workers
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y python3.8 python3.8-dev python3.8-distutils curl && \
    curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py -o get-pip.py && \
    python3.8 get-pip.py && \
    rm get-pip.py && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and sgp4 for Python 3.8
RUN python3.8 -m pip install --no-cache-dir pyspark==3.5.0 sgp4

# Update python3 symlink to point to python3.8
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 2 && \
    update-alternatives --set python3 /usr/bin/python3.8

USER $NB_UID
