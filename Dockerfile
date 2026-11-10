FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY retryguard ./retryguard
COPY services ./services
COPY experiments ./experiments
EXPOSE 8100 8101
