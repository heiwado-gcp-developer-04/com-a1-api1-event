FROM python:3.13-slim-trixie

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN useradd -u 8082 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080
CMD ["functions-framework", "--target", "trigger_workflow", "--port", "8080"]
