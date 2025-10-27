.PHONY: dev-up dev-down health daily-run fmt lint type test precommit-install

export TP_ENV=DEV

# One-click local stack
COMPOSE_FILE := infra/docker/docker-compose.yml
# 你的 compose 项目名目前显示为 docker（网络叫 docker_default）
# 如果你改了项目名，请把 NETWORK 换成 <project>_default
NETWORK := docker_default

dev-up:
	@echo "▶ Starting stack ($(TP_ENV))..."
	@docker compose -f $(COMPOSE_FILE) up -d --build

	@echo "▶ Waiting for MinIO to be ready..."
	@bash -lc 'for i in {1..60}; do curl -sf http://localhost:9000/minio/health/ready >/dev/null && exit 0; sleep 1; done; echo "MinIO not ready after 60s" >&2; exit 1'

	@echo "▶ Initializing MinIO buckets (raw, clean) via minio/mc..."
	@docker run --rm --network $(NETWORK) \
	  -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
	  minio/mc mb -p minio/raw || true
	@docker run --rm --network $(NETWORK) \
	  -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
	  minio/mc mb -p minio/clean || true

	@echo "✅ dev-up done."

dev-down:
	@docker compose -f infra/docker/docker-compose.yml down

# 列出 MinIO 的桶和对象
.PHONY: minio-ls
minio-ls:
	@echo "▶ Listing buckets (compose network: $(NETWORK))"
	@docker run --rm --network $(NETWORK) \
	  -e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
	  minio/mc ls minio

# 健康检查：API /healthz、MinIO 就绪、Postgres/ClickHouse 端口
.PHONY: health
health:
	@echo "▶ API healthz"
	@curl -sSf http://localhost:8000/healthz | jq . || { echo "❌ API health check failed"; exit 1; }
	@echo "▶ MinIO ready probe"
	@curl -sSf http://localhost:9000/minio/health/ready >/dev/null && echo "✅ MinIO ready" || { echo "❌ MinIO not ready"; exit 1; }
	@echo "▶ Postgres TCP check (:5432)"
	@bash -lc 'nc -z localhost 5432 && echo "✅ Postgres ready" || (echo "❌ Postgres not reachable"; exit 1)'
	@echo "▶ ClickHouse HTTP check (:8123)"
	@curl -sSf http://localhost:8123 >/dev/null && echo "✅ ClickHouse HTTP OK" || { echo "❌ ClickHouse HTTP failed"; exit 1; }
# End-to-end run for AAPL (acceptance)
daily-run:
	@poetry run python apps/workers/tasks/run_daily_pipeline.py
	@echo "CSV report(s) in ./data/reports/"

fmt:
	@poetry run ruff check . --fix

lint:
	@poetry run ruff check .

type:
	@poetry run mypy .

test:
	@poetry run pytest -q

precommit-install:
	@poetry run pre-commit install
ps:
	\tdocker compose -f infra/docker/docker-compose.yml ps

up:
	\tdocker compose -f infra/docker/docker-compose.yml up -d --build api

logs:
	\tdocker compose -f infra/docker/docker-compose.yml logs -f --tail=100 api

contracts-gen:
	\tpython libs/contracts/generator/gen_pydantic.py

sync:
	bash sync_code.sh