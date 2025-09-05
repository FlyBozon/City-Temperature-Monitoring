FROM bitnami/spark:3.5.2
USER root
RUN install_packages python3 python3-pip
USER 1001
