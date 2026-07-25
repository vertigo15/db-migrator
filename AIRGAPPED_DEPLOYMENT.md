# Air-Gapped AMD64 Deployment

This guide packages the migrator on an internet-connected machine and runs it
on an air-gapped Linux AMD64 server.

## 1. Check the on-prem environment

On the PostgreSQL server, check available CPU cores:

```bash
nproc
```

In PostgreSQL, check connection capacity and current usage:

```sql
SHOW max_connections;
SELECT count(*) AS current_connections FROM pg_stat_activity;
SELECT count(*) AS waiting_locks FROM pg_locks WHERE NOT granted;
```

Suggested starting worker counts:

- 4 DB cores: 4–8 workers
- 8 DB cores: 8–16 workers
- 16+ DB cores with fast storage: 16–32 workers

Each worker can use multiple PostgreSQL connections. Start conservatively and
increase workers while monitoring CPU, disk latency, connections, and waiting
locks.

Also confirm that the Docker host can reach:

- The V4 source PostgreSQL server
- The V5 `user_db`, `document_db`, and `completion_db` databases
- TCP port `5432` on those servers

## 2. Build the AMD64 image

Run from the repository root on an internet-connected machine:

```bash
docker buildx build \
  --platform linux/amd64 \
  --tag jeen-db-migrator:latest \
  --load \
  .

docker image inspect jeen-db-migrator:latest \
  --format '{{.Os}}/{{.Architecture}}'
```

The final command must print:

```text
linux/amd64
```

Export the image and prepare one transfer bundle:

```bash
docker save --output jeen-db-migrator-amd64.tar jeen-db-migrator:latest

mkdir -p airgap-bundle
cp jeen-db-migrator-amd64.tar docker-compose.yml .env.example \
  AIRGAPPED_DEPLOYMENT.md airgap-bundle/

tar -cvf db-migrator-airgap-bundle.tar airgap-bundle/
```

Transfer `db-migrator-airgap-bundle.tar` into the air-gapped environment
using the approved process.

## 3. Install on the air-gapped server

Docker Engine and the Docker Compose plugin must already be installed.

Unpack the received bundle:

```bash
sudo mkdir -p /opt/db-migrator
sudo tar -xvf db-migrator-airgap-bundle.tar \
  -C /opt/db-migrator --strip-components=1
cd /opt/db-migrator
```

Load and verify the image:

```bash
docker load --input jeen-db-migrator-amd64.tar
docker image inspect jeen-db-migrator:latest \
  --format '{{.Os}}/{{.Architecture}}'
```

Create the configuration and persistent directories:

```bash
cp .env.example .env
chmod 600 .env
mkdir -p output/extract output/migrations configs backups
```

Edit `.env` and set at least:

```dotenv
SOURCE_DB_HOST=<v4-host>
SOURCE_DB_PORT=5432
SOURCE_DB_DATABASE=<v4-database>
SOURCE_DB_USERNAME=<v4-user>
SOURCE_DB_PASSWORD=<v4-password>
TABLE_PREFIX=jeen_dev

TARGET_DB_HOST=<v5-host>
TARGET_DB_PORT=5432
TARGET_DB_DATABASE=user_db
TARGET_DB_USERNAME=<v5-user>
TARGET_DB_PASSWORD=<v5-password>
TARGET_SCHEMA_MODE=databases

DEFAULT_ORG_ID=<target-organization-uuid>
```

## 4. Start and verify

Start the UI and eight workers without attempting an offline image build:

```bash
docker compose up -d --no-build --scale migration-worker=8
docker compose ps
curl --fail http://localhost:8501/_stcore/health
```

The health request should return `ok`. Open:

```text
http://<docker-host-ip>:8501
```

In the UI:

1. Test the V4 source connection.
2. Verify the V5 target databases.
3. Migrate one light test user.
4. Test one data-heavy user.
5. Increase the batch size or worker count only after checking server load.

## 5. Operations

Scale workers:

```bash
docker compose up -d --no-build --scale migration-worker=16
```

Follow logs:

```bash
docker compose logs -f db-migrator migration-worker
```

Restart after changing `.env`:

```bash
docker compose up -d --no-build --force-recreate \
  --scale migration-worker=8
```

Stop workers gracefully:

```bash
docker compose stop migration-worker
```

Stop the entire application:

```bash
docker compose down
```

Do not delete `output/` while migrations are queued or running; the workers
read generated SQL shards from that directory.
