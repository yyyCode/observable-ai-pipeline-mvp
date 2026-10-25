.PHONY: install run test demo reset docker

install:
	python -m pip install -r requirements.txt

run:
	python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

test:
	python -m pytest tests/ -q

demo:
	python scripts/demo.py all

reset:
	python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('data', 'logs')]"

docker:
	docker compose up --build
