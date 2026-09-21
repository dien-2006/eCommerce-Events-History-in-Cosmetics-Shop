#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
python - <<'PYTHON'
import json, os, subprocess
users = json.loads(subprocess.check_output(['airflow', 'users', 'list', '--output', 'json']))
name = os.environ['AIRFLOW_ADMIN_USER']
if not any(user['username'] == name for user in users):
    subprocess.run(['airflow', 'users', 'create', '--username', name,
                    '--password', os.environ['AIRFLOW_ADMIN_PASSWORD'],
                    '--firstname', 'Platform', '--lastname', 'Admin',
                    '--role', 'Admin', '--email', os.environ['AIRFLOW_ADMIN_EMAIL']], check=True)
PYTHON
airflow pools set lakehouse_writes 1 "Serialize lakehouse writes on this host"
