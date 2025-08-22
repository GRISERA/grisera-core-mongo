#!/bin/bash

pip install -e /app/grisera-api-dev-packages

# Start the application
uvicorn main:app --reload --host 0.0.0.0 --port 80
