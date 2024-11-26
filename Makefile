# don't forget to take a look in .env file
init:
	docker compose up airflow-init

up:
	docker compose up

down:
	docker compose down

restart:
	docker-compose restart

reset:
	docker compose down --volumes --rmi all
