
FROM python:3.12.3

#Setting the working directory within the container.
WORkDIR /app

#Copying the current working dir into the  /app folder
COPY . /app

#Installing dependencies
RUN pip install pandas

#On starting what should the container do...
ENTRYPOINT ["python","calculator_output.py"]