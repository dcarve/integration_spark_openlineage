FROM python:3.8
RUN apt-get update

# We copy just the requirements.txt first to leverage Docker cache
COPY app/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY app/ /

EXPOSE 8086

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8086", "--reload"]