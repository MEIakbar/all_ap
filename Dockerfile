# Pull base image
FROM python:3.7

RUN mkdir /app
ADD . /app/
WORKDIR /app

RUN pip3 install -r requirements.txt

EXPOSE 8090
CMD ["python", "api.py"]
