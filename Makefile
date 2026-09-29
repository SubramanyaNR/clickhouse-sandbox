.PHONY: up up-monitoring linked schema load kafka kafka-stop cluster shell query \
        break-server break-keeper break-catchup heal urls ps down clean

# Once streaming-net exists, every target includes the linked overlay.
# Otherwise a plain `docker compose up` of one service recreates the services
# it depends on without the shared network and cuts off the other stacks.
LINKED = $(shell docker network inspect streaming-net >/dev/null 2>&1 && echo "-f docker-compose.linked.yml")
COMPOSE = docker compose -f docker-compose.yml $(LINKED)
DAYS ?= 90

up:
	$(COMPOSE) up -d --wait
	bin/ch schema
	@$(MAKE) --no-print-directory urls

up-monitoring:
	$(COMPOSE) --profile monitoring up -d

linked:
	docker network inspect streaming-net >/dev/null 2>&1 || docker network create streaming-net
	docker compose -f docker-compose.yml -f docker-compose.linked.yml up -d --wait
	bin/ch schema

schema:
	bin/ch schema

# make load DAYS=180 for more history
load:
	bin/ch load $(DAYS)

# Start consuming bank.dbo.transactions from kafka-sandbox (linked mode).
kafka:
	@for s in ch-server1 ch-server2 ch-server3; do \
	  docker exec -i $$s clickhouse-client --multiquery < integrations/kafka.sql && echo "$$s: consuming"; \
	done

kafka-stop:
	@for s in ch-server1 ch-server2 ch-server3; do \
	  docker exec $$s clickhouse-client --multiquery -q "DROP VIEW IF EXISTS bank.transactions_kafka_mv; DROP TABLE IF EXISTS bank.transactions_kafka" && echo "$$s: stopped"; \
	done

cluster:
	@bin/ch cluster

shell:
	bin/ch shell $(or $(N),1)

# make query Q=queries/05-planted-anomalies.sql  or  Q="SELECT count() FROM bank.loans"
query:
	@bin/ch query "$(Q)"

break-server:
	bin/break server $(or $(N),2)

break-keeper:
	bin/break keeper

break-catchup:
	bin/break catchup

heal:
	bin/break heal

urls:
	@. ./.env; \
	echo "clickhouse http   http://localhost:$${PORT_PREFIX}123 (ch-server1), $${PORT_PREFIX}124, $${PORT_PREFIX}125"; \
	echo "clickhouse native localhost:$${PORT_PREFIX}900"; \
	echo "play ui           http://localhost:$${PORT_PREFIX}123/play"; \
	echo "grafana           http://localhost:$${PORT_PREFIX}300"; \
	echo "prometheus        http://localhost:$${PORT_PREFIX}090"

ps:
	@$(COMPOSE) --profile monitoring ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'

down:
	$(COMPOSE) --profile monitoring --profile tools down

clean:
	$(COMPOSE) --profile monitoring --profile tools down -v
