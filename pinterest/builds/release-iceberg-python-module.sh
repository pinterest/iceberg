#!/bin/bash
# Script to release iceberg python module. It is used within
# https://jenkinsmaster-015.pinadmin.com/job/release-iceberg-python-module/

set -x
set -e

DOCKER_IMAGE=998131032990.dkr.ecr.us-east-1.amazonaws.com/jenkins:iceberg-build-pinterest-openjdk8_latest
docker pull $DOCKER_IMAGE

cd $WORKSPACE

ICEBERG_MAIN_VERSION=1.0
ICEBERG_FULL_VERSION=${ICEBERG_MAIN_VERSION}+pinterest${BUILD_NUMBER}
APPEND_LINE="__version__ = '${ICEBERG_FULL_VERSION}'"
VERSION_FILE_PATH='python_legacy/iceberg/__init__.py'
echo $APPEND_LINE >> $VERSION_FILE_PATH

docker run --rm -v $WORKSPACE:/iceberg -e CI_USER=$(id -u) -e CI_GROUP=$(id -g) $DOCKER_IMAGE /bin/sh -c \
  "cd /iceberg/python_legacy; tox -e py38 --notest; chown -R \${CI_USER}:\${CI_GROUP} .tox"

artifactory-push python \
  --env prod \
  --region use1 \
  --pypy_name "py-iceberg" \
  --pypy_version "${ICEBERG_FULL_VERSION}" \
  --src "$WORKSPACE/python_legacy/.tox/.pkg/dist/py-iceberg-${ICEBERG_FULL_VERSION}.tar.gz" \
  --to_repo pinterest-python-pip-prod-local

git reset --hard HEAD
