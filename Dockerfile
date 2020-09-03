FROM python:3

ADD . /pedgraph

WORKDIR /pedgraph

RUN pip install -e .

ENTRYPOINT ["python3"]

# If I want to test with "docker run":
#CMD ["pedgraph/test/test.py"]
# If I want to test in "docker build":
#RUN "./pedgraph/test/test.py"
