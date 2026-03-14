CREATE DATABASE IF NOT EXISTS rest.cosmetics_smoke;

CREATE TABLE IF NOT EXISTS rest.cosmetics_smoke.t1 (
  id INT,
  ts TIMESTAMP,
  note STRING
) USING iceberg
PARTITIONED BY (days(ts));

INSERT INTO rest.cosmetics_smoke.t1 VALUES (1, current_timestamp(), 'hello iceberg');

SELECT count(*) AS cnt FROM rest.cosmetics_smoke.t1;
SELECT * FROM rest.cosmetics_smoke.t1 LIMIT 5;