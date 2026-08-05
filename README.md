# Notification System

A modular notification platform that receives events via an API gateway, produces/consumes Kafka messages, and delivers notifications (email, SMS, push). It includes a React/Vite frontend for user interaction and Python microservices for gateway, notification processing, and user management.

## Stack
- Languages: Python (backend), JavaScript (React/Vite frontend), CSS
- Runtime / frameworks:
  - Backend: Python (plain modules / small services)
  - Frontend: React + Vite
- Notable libraries & tooling (shape the codebase):
  - Kafka client (producer/consumer logic in notification_service)
  - Redis client (api_gateway/redis_client.py)
  - FastAPI/Flask-style lightweight routing patterns (api_gateway/routes/)
  - Vite + React for the Frontend (Frontend/package.json)
  - Docker & docker-compose for local orchestration

## What’s in this repository
Top-level layout (annotated):

.dockerignore
.gitignore
docker-compose.yml            # orchestrates multiple services
test_email.py                 # quick email delivery test script
Frontend/                     # React + Vite frontend
  package.json
  vite.config.js
  src/
    App.jsx, main.jsx, pages/  # UI pages and components (Login, Register, Form)
    components/                # GrantApplicationForm, LoginForm, RegisterForm, ResultModal
api_gateway/                   # Gateway handling auth, rate limiting, middleware, routes
  main.py
  middleware.py
  auth.py
  config.py
  redis_client.py
  routes/
    public.py
    private.py
  requirements.txt
notification_service/          # Notification microservice (producers, consumers, DB models)
  main.py
  kafka_producer.py
  create_topics.py
  database.py
  models.py
  schemas.py
  requirements.txt
  notification_consumer/       # Consumers: email, sms, push, retry, DLQ, user_consumer
user_service/                  # user-related service (API for user data)

How it fits together:
- The api_gateway exposes the public API (api_gateway/routes/public.py) and protected routes (api_gateway/routes/private.py). It handles authentication, rate limiting, and forwards events to internal services or Kafka.
- notification_service produces/consumes Kafka topics (kafka_producer.py, create_topics.py) and runs workers/consumers in notification_consumer/* to deliver notifications (email_consumer.py, sms_consumer.py, push_consumer.py). database.py and models.py define persistence models.
- The Frontend (Vite React app) provides the UI pages and components to interact with the gateway (login/register, grant application form).

## Quick start — recommended (Docker Compose)
This repository includes a docker-compose.yml that brings up services for local development (API gateway, notification service, Kafka, Redis, etc.).

1. Clone the repo
   git clone https://github.com/AadyaPandey/notification_system.git
   cd notification_system

2. Build and run with docker-compose
   docker-compose up --build

3. Verify:
   - Frontend: open http://localhost:3000 (or as configured in Frontend/vite.config.js)
   - API gateway: check the logs for `api_gateway` service
   - notification_service consumers: check logs for delivery attempts

(Adjust ports/envs in docker-compose.yml if they conflict with your host.)

## Run components individually (for development)
- API Gateway
  1. cd api_gateway
  2. pip install -r requirements.txt
  3. python main.py
  - Files of interest: api_gateway/main.py, api_gateway/middleware.py, api_gateway/routes/*.py

- Notification Service
  1. cd notification_service
  2. pip install -r requirements.txt
  3. python main.py
  - Files of interest: notification_service/main.py, kafka_producer.py, create_topics.py and notification_consumer/*

- Frontend (dev)
  1. cd Frontend
  2. npm install
  3. npm run dev
  - Entry files: Frontend/src/main.jsx, Frontend/src/App.jsx; pages in Frontend/src/pages/

## Environment & configuration
Configuration is centralized in api_gateway/config.py and service-level requirements files (api_gateway/requirements.txt, notification_service/requirements.txt). The docker-compose.yml also documents which services need which environment variables.

Common environment variables you will likely need to set (refer to api_gateway/config.py and notification_service/* for exact names):
- DATABASE_URL (DB connection)
- KAFKA_BOOTSTRAP_SERVERS (or broker addresses)
- REDIS_URL
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD (for email delivery)
- SECRET_KEY / JWT configuration (for auth)

Always check api_gateway/config.py and notification_service/notification_consumer/email_consumer.py for the exact variable names before running.

## Testing
- A small test harness exists: test_email.py — can be used to validate email sending configured in notification_service.
  python test_email.py

- To test consumers, run notification_service with a running Kafka broker and publish test messages (create_topics.py helps prepare topics).

## Important files to inspect
- api_gateway/main.py — gateway entrypoint and server setup
- api_gateway/middleware.py — auth & request middleware logic
- api_gateway/routes/public.py, private.py — public/private API routes
- notification_service/notification_consumer/email_consumer.py — email delivery flow & retry handling
- notification_service/retry_consumer.py, dlq_consumer.py — retry and dead-letter handling
- Frontend/src/components/* — UI components (LoginForm, RegisterForm, GrantApplicationForm)

## Contributing
- Open an issue describing the change.
- Create a feature branch, add tests where applicable.
- Submit a pull request with a clear description and testing steps.

## License
Specify a license here (NONE included in repo). Add a LICENSE file if you want to make the project open source.

## Contact / Questions
If you need help setting up local dependencies (Kafka, Redis, DB) or configuring SMTP creds, refer to:
- docker-compose.yml (service definitions & ports)
- api_gateway/config.py
- notification_service/* for consumer logic

## Try asking
- How should I configure SMTP credentials so email_consumer.py picks them up (where in notification_service are they referenced)?
- Can you add unit tests for notification_service/notification_consumer/retry_consumer.py and a small CI job to run them?
- Where does api_gateway route requests to the user_service (which route file and path) and how is authentication checked before forwarding?
