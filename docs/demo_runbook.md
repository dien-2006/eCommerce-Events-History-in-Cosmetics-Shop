# Demo Runbook (Presentation Day)

## A) Boot infra
```bash
bash scripts/download_jars.sh
docker compose up -d --build
bash scripts/verify_services.sh