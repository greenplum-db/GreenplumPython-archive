FROM postgres:12-bookworm

COPY build.sh initdb.sh /tmp/
RUN bash /tmp/build.sh
