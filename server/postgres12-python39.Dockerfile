FROM postgres:12-bullseye

COPY build.sh /tmp/build.sh
RUN bash /tmp/build.sh && rm -rf /tmp/build.sh
