.PHONY: build test format check up down image-local benchmark verify-observability verify-deployment run-job integration-test recovery-test idle-resume-test generate-load inject-duplicates inject-late-events spike-load kill-taskmanager savepoint restore stop-job topics schemas dashboards
build:
	mvn -B verify
benchmark:
	python3 scripts/benchmark.py
verify-observability:
	python3 scripts/verify-observability.py
verify-deployment:
	python3 scripts/verify-deployment.py
image-local: build
	docker build --target local -f infrastructure/docker/Dockerfile -t payment-risk:0.1.0 .
test:
	mvn -B test
format:
	mvn -B spotless:apply
check:
	mvn -B spotless:check verify
up:
	python3 scripts/init-env.py
	docker compose up -d --build
down:
	docker compose down
run-job:
	python3 scripts/operations.py submit
integration-test:
	python3 scripts/operations.py integration
recovery-test:
	python3 scripts/operations.py recovery-test
idle-resume-test:
	python3 scripts/operations.py idle-resume-test
generate-load:
	docker compose exec -T jobmanager java -cp /opt/flink/usrlib/risk-engine.jar com.portfolio.paymentrisk.tools.PlatformCli generate steady 10000 100
inject-duplicates:
	docker compose exec -T jobmanager java -cp /opt/flink/usrlib/risk-engine.jar com.portfolio.paymentrisk.tools.PlatformCli generate duplicates 100 100
inject-late-events:
	docker compose exec -T jobmanager java -cp /opt/flink/usrlib/risk-engine.jar com.portfolio.paymentrisk.tools.PlatformCli generate late 100 100
spike-load:
	docker compose exec -T jobmanager java -cp /opt/flink/usrlib/risk-engine.jar com.portfolio.paymentrisk.tools.PlatformCli generate burst 10000 1000
kill-taskmanager:
	python3 scripts/operations.py recover
savepoint:
	python3 scripts/operations.py savepoint
stop-job:
	python3 scripts/operations.py stop
restore:
	python3 scripts/operations.py restore
topics schemas:
	docker compose exec -T jobmanager java -cp /opt/flink/usrlib/risk-engine.jar com.portfolio.paymentrisk.tools.PlatformCli bootstrap
dashboards:
	@echo http://localhost:23000

.PHONY: website website-check
website:
	docker compose up -d --build --no-deps website
website-check:
	npm --prefix web ci
	npm --prefix web test
	npm --prefix web run build
