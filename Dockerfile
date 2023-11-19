FROM docker.io/mambaorg/micromamba:1.5-bullseye AS base
ARG NEW_MAMBA_USER=jovian
ARG NEW_MAMBA_USER_ID=1000
ARG NEW_MAMBA_USER_GID=1000
ARG MAMBA_DOCKERFILE_ACTIVATE=1 
USER root

RUN if grep -q '^ID=alpine$' /etc/os-release; then \
      # alpine does not have usermod/groupmod
      apk add --no-cache --virtual temp-packages shadow; \
    fi && \
    usermod "--login=${NEW_MAMBA_USER}" "--home=/home/${NEW_MAMBA_USER}" \
        --move-home "-u ${NEW_MAMBA_USER_ID}" "${MAMBA_USER}" && \
    groupmod "--new-name=${NEW_MAMBA_USER}" \
        "-g ${NEW_MAMBA_USER_GID}" "${MAMBA_USER}" && \
    if grep -q '^ID=alpine$' /etc/os-release; then \
      # remove the packages that were only needed for usermod/groupmod
      apk del temp-packages; \
    fi && \
    # Update the expected value of MAMBA_USER for the
    # _entrypoint.sh consistency check.
    echo "${NEW_MAMBA_USER}" > "/etc/arg_mamba_user" && \
    :

# Create and set the workspace folder
ARG CONTAINER_WORKSPACE_FOLDER=/workspaces/ecovoyage
RUN mkdir -p "${CONTAINER_WORKSPACE_FOLDER}"
WORKDIR "${CONTAINER_WORKSPACE_FOLDER}"
ENV MAMBA_USER=$NEW_MAMBA_USER
USER $MAMBA_USER


FROM base AS core
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_core.yaml /tmp/env_core.yaml
RUN micromamba install -y -f /tmp/env_core.yaml && micromamba clean --all --yes


FROM core AS jupyter
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 
#COPY --from=core /opt/conda /opt/conda
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_jupyter.yaml /tmp/env_jupyter.yaml
RUN micromamba install -y -f /tmp/env_jupyter.yaml && micromamba clean --all --yes


FROM jupyter AS ai
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 
#COPY --from=jupyter /opt/conda /opt/conda
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_ai.yaml /tmp/env_ai.yaml 
RUN micromamba install -y -f /tmp/env_ai.yaml && micromamba clean --all --yes


FROM ai AS spatial
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 
#COPY --from=ai /opt/conda /opt/conda
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_spatial.yaml /tmp/env_spatial.yaml 
RUN micromamba install -y  -f /tmp/env_spatial.yaml && micromamba clean --all --yes


FROM spatial AS testing
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 
#COPY --from=spatial /opt/conda /opt/conda
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_testing.yaml /tmp/env_testing.yaml
RUN micromamba install -y -f /tmp/env_testing.yaml && micromamba clean --all --yes


FROM testing as devel
#ARG MAMBA_DOCKERFILE_ACTIVATE=1 

ARG DOCKER_GID=999

#COPY --from=testing /opt/conda /opt/conda
USER root
RUN apt-get update && apt-get install -y build-essential openssh-client rsync sudo git apt-transport-https vim \
    ca-certificates curl gnupg lsb-release software-properties-common && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN touch /var/lib/dpkg/status && install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null && apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN usermod -aG sudo $MAMBA_USER && echo 'jovian ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN groupmod -g ${DOCKER_GID}  docker && sudo usermod -aG docker jovian

USER $MAMBA_USER
