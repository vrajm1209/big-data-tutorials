FROM python:3.8.10
#pull the base docker image with tag 3.8.9 since thats the version in my venv

###3 now you are inside the container

WORKDIR /app
#change the working dir inside the container 

#copy your local files into the container 
#. means current dir
#main.py is the source code
#req.txt is the dependancies
COPY main.py requirements.txt ./

#install dependencies
RUN pip install -r requirements.txt

#### container env same as local at this point

EXPOSE 8081

## CMD ["gunicorn" ,"-w", "4", "-k", "uvicorn.workers.UvicornWorker" , "--bind", "0.0.0.0:8080", "main:app"]
# gunicorn command to run the service with 4 worker nodes binding localhost/0.0.0.0 on port 8000 refering app inside the main.py

CMD ["streamlit", "run", "main.py", "--server.port", "8081"]