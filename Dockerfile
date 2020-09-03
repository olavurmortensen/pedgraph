FROM python:3

ADD . /pedgraph

WORKDIR /pedgraph

RUN pip install -e .

# CircleCI will over-write the entry point with:
# #!/bin/bash -eo pipefail
# We can use this to preserver the entrypoint. For more information see:
# https://circleci.com/docs/2.0/custom-images/#adding-an-entrypoint
LABEL com.circleci.preserve-entrypoint=true

ENTRYPOINT ["python3"]

# If I want to test with "docker run":
#CMD ["pedgraph/test/test.py"]
# If I want to test in "docker build":
#RUN "./pedgraph/test/test.py"
