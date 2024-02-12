FROM python:3.12-slim-bookworm
ENV APP_PATH /opt/apps
ENV DEBIAN_FRONTEND noninteractive
ENV HOME /home/app
ENV PATH $HOME/.local/bin:$PATH

USER root

COPY . ${APP_PATH}
WORKDIR ${APP_PATH}

RUN groupadd -r app && useradd -r -g app app
RUN mkdir -p ${HOME}
RUN set -e && bash build.sh
RUN chown -R app:app ${APP_PATH}
RUN chown -R app:app ${HOME}

USER app
WORKDIR ${APP_PATH}

ENTRYPOINT [ "/bin/bash", "-c" ]
CMD [ "python" ]
