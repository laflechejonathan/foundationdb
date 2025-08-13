# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM docker.palantir.build/ubuntu-nano-installer:u22.04-11202 AS base

WORKDIR /tmp

ARG TARGETARCH
ARG FDB_VERSION=7.3.69
ARG FDB_LIBRARY_VERSIONS="${FDB_VERSION}"
ARG FDB_WEBSITE=https://artifactory.palantir.build/artifactory/external-dist-github-com/apple/foundationdb/releases/download

RUN apt-get update && \
    apt-get install -y curl

RUN curl -Ls "https://artifactory.palantir.build/artifactory/external-dist-github-com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH" -o "tini-$TARGETARCH"  && \
    echo "93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c  tini-amd64" > "tini-amd64-sha.txt" && \
    echo "07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81  tini-arm64" > "tini-arm64-sha.txt" && \
    sha256sum --quiet -c "tini-${TARGETARCH}-sha.txt" && \
    chmod +x "tini-$TARGETARCH" && \
    mv "tini-$TARGETARCH" /usr/bin/tini && \
    rm -rf /tmp/*

COPY website /tmp/website/

# Install FoundationDB Binaries and additional FoundationDB Client Libraries
# We need to do this here so we have access to curl
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        FDB_ARCH=x86_64; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        FDB_ARCH=aarch64; \
    else \
        echo "ERROR: unsupported architecture $TARGETARCH" 1>&2; \
        exit 1; \
    fi; \
    mkdir -p /downloads/bin /downloads/lib /downloads/multiversion && \
    CURL="curl --fail -Ls"; \
    for file in fdbserver fdbbackup fdbcli fdbmonitor; do \
        URL="${FDB_WEBSITE}/${FDB_VERSION}/$file.$FDB_ARCH"; \
        echo "Downloading $URL"; \
        TARGET="/downloads/bin/$file"; \
        $CURL "$URL" -o "$TARGET" && \
        chmod +x "$TARGET"; \
    done && \
    for version in $FDB_LIBRARY_VERSIONS; do \
        URL="${FDB_WEBSITE}/${version}/libfdb_c.${FDB_ARCH}.so"; \
        echo "Downloading $URL"; \
        TARGET="/downloads/multiversion/libfdb_c_${version%.*}.so"; \
        $CURL "$URL" -o "$TARGET"; \
    done && \
    $CURL $FDB_WEBSITE/$FDB_VERSION/libfdb_c.${FDB_ARCH}.so -o /downloads/lib/libfdb_c.so


WORKDIR /

FROM hub.docker.palantir.build/library/golang:1.24.6-bookworm AS go-build
COPY fdbkubernetesmonitor/ /fdbkubernetesmonitor
WORKDIR /fdbkubernetesmonitor
RUN go build -o /fdb-kubernetes-monitor *.go

FROM docker.palantir.build/ubuntu-nano:u22.04-11202 AS foundationdb-base

COPY --from=base /usr/bin/tini /usr/bin/tini

ARG FDB_VERSION=6.3.22

RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    mkdir -p /var/fdb/{logs,tmp,lib} && \
    mkdir -p /usr/lib/fdb/multiversion && \
    echo ${FDB_VERSION} > /var/fdb/version

RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    apt-get update && apt-get install pkg-config libc6 liblzma5 -y --no-install-recommends && \
    apt-get clean

# Set up a non-root user
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    groupadd --gid 4059 \
             fdb && \
    useradd --gid 4059 \
            --uid 4059 \
            --no-create-home \
            --shell /bin/bash \
            fdb && \
    chown -R fdb:fdb /var/fdb

WORKDIR /tmp

# Copy over the FDB binaries and libraries
COPY --from=base /downloads/bin/* /usr/bin/
COPY --from=base /downloads/lib/* /usr/lib/
COPY --from=base /downloads/multiversion/* /var/fdb/lib/
COPY --from=base /downloads/multiversion/* /usr/lib/fdb/multiversion/

# Setup all symlinks for the other binaries that are a copy of fdbbackup
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    for file in fdbdr fdbrestore backup_agent dr_agent fastrestore_tool; do \
        ln -s /usr/bin/fdbbackup "/usr/bin/$file"; \
    done && \
    cd / && \
    rm -rf /tmp/*

# ubuntu-nano has very restrictive default folder permissions. We need to allow the fdb user to be able to access the
# /var/log/fdb-trace-logs folder.
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    chmod o+x /var/log

WORKDIR /

FROM foundationdb-base AS fdb-kubernetes-monitor

# Install the kubernetes monitor binary
COPY --from=go-build /fdb-kubernetes-monitor /usr/bin/

# Runtime Configuration Options
USER fdb
WORKDIR /var/fdb
VOLUME /var/fdb/data
ENTRYPOINT ["/usr/bin/fdb-kubernetes-monitor"]

FROM foundationdb-base AS foundationdb-kubernetes-sidecar

# We use python3.9 here as the tests with 3.12 resulted in the following error:
# error:0A000126:SSL routines::unexpected eof while reading. This could be an issue
# with the installed openssl version. Since the sidecar support will be deprecated in the
# future it's fine to use python 3.9 here.
# EOL for python 3.9 is 2025-10: https://devguide.python.org/versions/

# We need to remove python3-pip to avoid several CVE
# We also upgrade from 3.9 to 3.10, since 3.9 is close to EOL
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    apt-get update && apt-get install -y \
        python3.10 python3-pip && \
    apt-get clean && \
    python3.10 -m pip install watchdog==4.0.1 && \
    apt-get remove python3-pip

WORKDIR /
COPY sidecar.py /
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    chmod a+x /sidecar.py
USER fdb
VOLUME /var/input-files
VOLUME /var/output-files
ENV LISTEN_PORT=8080
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/sidecar.py"]

FROM foundationdb-base AS foundationdb

WORKDIR /
# Set Up Runtime Scripts and Directories
ADD fdb.bash fdb_single.bash /var/fdb/scripts/
RUN --mount=type=bind,from=base,source=/,target=/installer/ \
    chmod a+x /var/fdb/scripts/fdb.bash /var/fdb/scripts/fdb_single.bash
VOLUME /var/fdb/data
ENV FDB_PORT=4500
ENV FDB_CLUSTER_FILE=/var/fdb/fdb.cluster
ENV FDB_NETWORKING_MODE=container
ENV FDB_COORDINATOR=""
ENV FDB_COORDINATOR_PORT=4500
ENV FDB_CLUSTER_FILE_CONTENTS=""
ENV FDB_PROCESS_CLASS=unset
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/var/fdb/scripts/fdb.bash"]

FROM foundationdb-base AS ycsb
# This stage is only used for load tests, which is not part of our build pipeline.
# This stage is broken, since the commands are still for rocky linux but we are now based on ubuntu.

RUN microdnf -y install \
    java-11-openjdk java-11-openjdk-devel && \
    microdnf clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        AWS_ARCH=x86_64; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        AWS_ARCH=aarch64; \
    else \
        echo "ERROR: unsupported architecture $TARGETARCH" 1>&2; \
        exit 1; \
    fi; \
    NO_PROXY="" no_proxy="" curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/$TARGETARCH/kubectl -o kubectl && \
    echo "860c3d37a5979491895767e7332404d28dc0d7797c7673c33df30ca80e215a07  kubectl" > kubectl.txt && \
    sha256sum --quiet -c kubectl.txt && \
    mv kubectl /usr/local/bin/kubectl && \
    chmod 755 /usr/local/bin/kubectl && \
    curl -Ls https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}-2.7.34.zip -o "awscliv2-${AWS_ARCH}.zip" && \
    echo "daf9253f0071b5cfee9532bc5220bedd7a5d29d4e0f92b42b9e3e4c496341e88  awscliv2-x86_64.zip" > awscliv2-x86_64.txt && \
    echo "bcb195622f0956c8569736b95bff9cff7f5684e400e694a33661f2d498fbc799  awscliv2-aarch64.zip" > awscliv2-aarch64.txt && \
    sha256sum --quiet -c awscliv2-${AWS_ARCH}.txt && \
    unzip -qq awscliv2-${AWS_ARCH}.zip && \
    ./aws/install && \
    rm -rf /tmp/*

# TODO: Log4J complains that it's eating the HTracer logs.  Even without it, we get per-operation
# time series graphs of throughput, median, 90, 99, 99.9 and 99.99 (in usec).
ADD run_ycsb.sh run_ycsb_standalone.sh /usr/local/bin
RUN mkdir -p /var/log/fdb-trace-logs && \
    chmod +x /usr/local/bin/run_ycsb.sh && \
    chmod +x /usr/local/bin/run_ycsb_standalone.sh


ADD YCSB /YCSB
WORKDIR /YCSB
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/var/dynamic-conf/lib/multiversion/
ENV FDB_NETWORK_OPTION_TRACE_ENABLE=/var/log/fdb-trace-logs
ENV LD_LIBRARY_PATH=/var/dynamic-conf/lib/
ENV BUCKET=""
ENTRYPOINT ["run_ycsb.sh"]
