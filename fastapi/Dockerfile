FROM python:3.8.10

RUN pip install --upgrade pip

WORKDIR /app

ADD main.py requirements.txt /app/

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["gunicorn" ,"-w", "4", "-k", "uvicorn.workers.UvicornWorker" , "--bind", "0.0.0.0:8000", "main:app"]