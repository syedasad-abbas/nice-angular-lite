# dialerv2 auth-api

Separate auth backend for portal login + user creation.

## What it provides

- `POST /auth/login`
- `GET /auth/me`
- `POST /admin/users` (admin-only)
- `GET /admin/dashboard` (admin-only)
- `GET /healthz`

It stores portal users in Redis and also provisions FreeSWITCH directory keys:

- `fsdir:user:<extension>`
- `agent:ext:<extension>`
- `agent:<agentId>`

## Local run (containerized)

```bash
docker build -t syedasadabbas/dialerv2-auth-api:0.1 ./auth-api
docker run --rm -p 3100:3100 \
  -e REDIS_URI=redis://host.docker.internal:6379 \
  -e JWT_SECRET=replace-with-strong-secret \
  -e SEED_ADMIN_ON_START=true \
  -e SEED_ADMIN_USERNAME=admin \
  -e SEED_ADMIN_PASSWORD=Pass@123 \
  -e SEED_ADMIN_EXTENSION=1001 \
  syedasadabbas/dialerv2-auth-api:0.1
```

## Login test

```bash
curl -i -X POST http://127.0.0.1:3100/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"Pass@123"}'
```
