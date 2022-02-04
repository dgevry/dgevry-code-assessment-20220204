ARG IMAGE_VARIANT=alpine
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

# Update image
RUN apk update && apk upgrade --no-cache

RUN cat /etc/os-release
# alpine 3.15

# Utilities
RUN set -ex \
  && apk add --no-cache bash \
                        dos2unix \
                        git \
                        make \
                        tree 

# Install and upgrade pip
RUN python3 -m pip install --upgrade pip --user

# Pip modules
COPY requirements.txt /tmp/
RUN python3 -m pip install --requirement /tmp/requirements.txt --user

RUN export PATH=$HOME/.local/bin:$PATH
