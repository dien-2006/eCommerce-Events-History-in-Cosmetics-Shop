#!/usr/bin/env bash
set -euo pipefail
superset db upgrade
python - <<'PYTHON'
import os
from superset.app import create_app
from superset.extensions import appbuilder
app = create_app()
with app.app_context():
    sm = appbuilder.sm
    username = os.environ['SUPERSET_ADMIN_USER']
    if not sm.find_user(username=username):
        sm.add_user(username, 'Platform', 'Admin', 'admin@example.com',
                    sm.find_role('Admin'), password=os.environ['SUPERSET_ADMIN_PASSWORD'])
PYTHON
superset init
python /app/docker-init/provision.py
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --worker-class gthread --threads 4 \
  --timeout 120 --access-logfile - --error-logfile - 'superset.app:create_app()'
