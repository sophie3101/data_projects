FROM python:3.10-slim
# install PyFlink
COPY requirements.txt .
RUN python -m pip install --upgrade pip; \
    pip3 install --upgrade google-api-python-client; \
    pip3 install -r requirements.txt  --no-cache-dir;

WORKDIR .