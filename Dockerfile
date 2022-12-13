FROM 172.30.3.149/common/alpine_timezone_shanghai:3.11.3
MAINTAINER skyguard-bigdata
ARG SUB_MODULE
WORKDIR /
USER root
COPY bin/$SUB_MODULE /manager
RUN chmod +x /manager

CMD ["./manager"]
