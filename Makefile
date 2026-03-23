ROOT_DIR=$(PWD)/src/parquet_pages
THRIFT=$(ROOT_DIR)/parquet.thrift


.PHONY: gen clean build publish

gen:
	docker build -t thrift-gen .
	docker run --rm -v $(ROOT_DIR):/app thrift-gen \
		bash -c 'thrift --gen py:enum -out /app /app/*.thrift'

clean:
	rm -rf $(ROOT_DIR)/parquet tests/tmp build dist *.egg-info

build: clean gen
	python -m build

publish: build
	python -m twine upload --verbose dist/*

test: gen
	pip install -e .
	pytest tests/
