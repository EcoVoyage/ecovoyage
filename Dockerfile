FROM docker.io/mambaorg/micromamba:1.5-bullseye AS base
ARG NEW_MAMBA_USER=jovian
ARG NEW_MAMBA_USER_ID=1000
ARG NEW_MAMBA_USER_GID=1000
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

COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_core.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && micromamba clean --all --yes


FROM base AS jupyter

COPY --from=core /opt/conda /opt
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_jupyter.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && micromamba clean --all --yes


FROM base AS ai

COPY --from=jupyter /opt/conda /opt
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_ai.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && micromamba clean --all --yes


FROM base AS spatial

COPY --from=ai /opt/conda /opt
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_spatial.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && micromamba clean --all --yes


FROM base AS testing

COPY --from=spatial /opt/conda /opt
COPY --chown=$MAMBA_USER:$MAMBA_USER env/env_spatial.yaml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && micromamba clean --all --yes


FROM base as devel

COPY --from=testing /opt/conda /opt
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client rsync sudo git && rm -rf /var/lib/apt /var/lib/dpkg /var/lib/cache /var/lib/log
USER $MAMBA_USER
