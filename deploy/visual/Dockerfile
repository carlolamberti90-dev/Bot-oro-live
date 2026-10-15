FROM mcr.microsoft.com/playwright/python:v1.56.0-noble
WORKDIR /app
COPY deploy/visual/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY deploy/visual/ ./
USER pwuser
ENV PYTHONUNBUFFERED=1
CMD ["python", "server.py"]
