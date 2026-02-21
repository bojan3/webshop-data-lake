FROM bde2020/spark-master:3.0.1-hadoop3.2

USER root

RUN pip install "Flask<2"

COPY server.py /app/server.py
COPY spark-defaults.conf /spark/conf/spark-defaults.conf

EXPOSE 8000

CMD bash -c "\
  /spark/sbin/start-master.sh && \
  python /app/server.py \
"
