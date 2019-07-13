FROM mirror.gcr.io/library/python
LABEL maintainer "Nathaniel Watson nathan.watson86@gmail.com"

COPY . /sruns_monitor/

RUN pip install --upgrade pip && pip install /sruns_monitor

USER root

ENTRYPOINT ["srun-mon"]
