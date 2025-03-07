.PHONY: health
health: 
	docker compose exec redpanda-0 rpk cluster health

.PHONY: maintenance
maintenance:
	docker compose exec redpanda-0 rpk cluster maintenance status

.PHONY: start-maintenance
start-maintenance:
	docker compose exec redpanda-0 rpk cluster maintenance enable 1 --wait

.PHONY: stop-maintenance
stop-maintenance:
	docker compose exec redpanda-0 rpk cluster maintenance disable 1 
