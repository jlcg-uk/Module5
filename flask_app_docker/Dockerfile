FROM python:3.12-slim

WORkDIR /app

copy . /app

RUN pip install -r requirements.txt

#It makes sure that the container exposes port 5000. 
EXPOSE 5000  

ENTRYPOINT ["python","app.py"]