FROM bde2020/spark-master:3.0.1-hadoop3.2

USER root

RUN pip install "Flask<2"

COPY server.py /app/server.py

EXPOSE 8000

CMD bash -c "python /app/server.py"
