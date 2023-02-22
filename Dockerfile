FROM python:3.8.13-slim-bullseye

COPY requirements.txt requirements.test.txt /
RUN pip install -r requirements.test.txt

COPY pipelines pipelines

COPY tests tests

CMD ["bash"]
