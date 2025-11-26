FROM apache/airflow:2.10.5-python3.11

USER root

# Install Google Cloud SDK
RUN apt-get update && apt-get install -y curl apt-transport-https ca-certificates gnupg && \
    curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    echo "deb https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update -y && apt-get install -y google-cloud-sdk

USER airflow
