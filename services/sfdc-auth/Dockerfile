FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY auth.py app.py ./
EXPOSE 5002
CMD ["python", "app.py"]
