FROM  artifactory.platform9.horse/docker-local/pf9-py39-baseimg-alpine:pcd-graviton-v2
LABEL com.platform9.image-type=du

ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache

RUN apk update
RUN apk add bash
RUN apk add curl
RUN apk add pkgconfig
RUN apk add gcc
RUN apk add g++
RUN apk add dpkg
RUN apk add musl-dev
RUN apk add openssl
RUN apk add mariadb \
     mariadb-client \
     --no-cache mariadb-dev \
     mariadb-connector-c-dev \
     python3-dev

WORKDIR /root

COPY etc/ /etc/
COPY shared/ /usr/local/lib/python3.9/site-packages/shared
COPY migrate.cfg /usr/local/lib/python3.9/site-packages/hamgr/db/migrate.cfg
COPY dbmigrate.py /usr/local/lib/python3.9/site-packages/hamgr/dbmigrate.py
COPY hamgr-sdist.tgz \
     requirements.txt \
     /root/
COPY liveness_probe.py /root/

COPY init-region .
RUN chmod 755 init-region

RUN pip install setuptools==57.1.0 \
                pbr==3.1.1

RUN pip install hamgr-sdist.tgz -r requirements.txt

RUN mkdir -p /var/log/pf9/hamgr/
RUN mkdir -p /etc/pf9/hamgr/
# hamgr=9083
EXPOSE 9083

CMD \
    supervisord -c /etc/supervisord.conf

ARG VERSION
LABEL com.platform9.pf9_version=${VERSION}
ARG BUILD_ID
LABEL com.platform9.build=${BUILD_ID}
LABEL com.platform9.version="${VERSION}-${BUILD_ID}"
ARG BRANCH
LABEL com.platform9.branch=${BRANCH}
ARG APP_METADATA
LABEL com.platform9.app_metadata=${APP_METADATA}
