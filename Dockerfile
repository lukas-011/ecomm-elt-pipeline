FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY extract.py .
COPY transform.py .
COPY load.py .
COPY elt_script.py .

CMD ["python", "elt_script.py"]