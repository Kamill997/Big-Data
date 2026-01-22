FROM python:3.12.10
LABEL authors="luka9"
WORKDIR /app
COPY . .
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python","app.py"]