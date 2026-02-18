.PHONY: dev build test seed logs down clean

dev:
	docker-compose up --build

build:
	docker-compose build

test:
	go test ./...

seed:
	docker-compose run --rm seeder

logs:
	docker-compose logs -f crawler parser

down:
	docker-compose down

clean:
	docker-compose down -v
