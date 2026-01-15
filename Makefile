
logs:
	docker compose logs -f

stop:
	docker compose down

start:
	docker compose up -d --build

api_client:
	uvicorn src.main:app --reload


