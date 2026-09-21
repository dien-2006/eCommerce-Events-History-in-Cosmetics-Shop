"""Small Prometheus exporter backed by the PostgreSQL operational registry."""

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

from lakehouse.connections import postgres
from lakehouse.registry import Registry


def render_metrics(db):
    rows = db.execute("""
      SELECT project,
        coalesce(extract(epoch FROM max(finished_at) FILTER (WHERE status='success')), 0) AS success_ts,
        count(*) FILTER (WHERE status='success') AS successes,
        count(*) FILTER (WHERE status='failed') AS failures
      FROM ops.runs GROUP BY project
    """).fetchall()
    output = []
    for row in rows:
        project = row["project"]
        label = "{project=" + json.dumps(project) + "}"
        latest = db.execute(
            "SELECT status, metrics FROM ops.runs WHERE project=%s ORDER BY sequence_id DESC LIMIT 1",
            (project,),
        ).fetchone()
        age = db.execute(
            """
          SELECT coalesce(max(extract(epoch FROM now()-s.started_at)), 0) AS age
          FROM ops.stages s JOIN ops.runs r ON s.run_key=r.run_key
          WHERE r.project=%s AND s.status='running' AND r.status='running'
        """,
            (project,),
        ).fetchone()["age"]
        metrics = {
            "last_success_timestamp_seconds": row["success_ts"],
            "successful_runs": row["successes"],
            "failed_runs": row["failures"],
            "last_run_failed": int(latest["status"] == "failed"),
            "running_stage_age_seconds": age,
            "silver_rows": latest["metrics"].get("transform", {}).get("silver_rows", 0),
            "quarantine_rows": latest["metrics"].get("transform", {}).get("quarantine_rows", 0),
        }
        for name, value in metrics.items():
            output.append(f"lakehouse_{name}{label} {value}")
    return "\n".join(output) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return
        try:
            with postgres() as db:
                Registry(db).initialize()
                body = render_metrics(db).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            logging.exception("Metrics collection failed")
            self.send_error(503)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 9108), Handler).serve_forever()
