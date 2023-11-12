FROM postgres:12-bullseye

COPY build.sh initdb.sh /tmp/
RUN bash /tmp/build.sh
