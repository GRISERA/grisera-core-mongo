#!/bin/bash

if [ "$APP_ENV" = "local" ]; then
  mkdir -p /minio/recordings || exit 1
fi

pip install -e /app/grisera-api-dev-packages

# Start the application
uvicorn main:app --reload --host 0.0.0.0 --port 80
