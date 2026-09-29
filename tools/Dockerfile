FROM python:3.12-slim
WORKDIR /app
COPY bankgen/ bankgen/
RUN cd bankgen && python universe.py >/dev/null
COPY export.py ./
ENTRYPOINT ["python", "export.py"]
