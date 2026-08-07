FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY apps/health_exam_admin/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps /app/apps
COPY scripts /app/scripts

EXPOSE 8011

CMD ["uvicorn", "apps.health_exam_admin.main:app", "--host", "0.0.0.0", "--port", "8011"]
