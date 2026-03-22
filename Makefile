PACKAGE=parquet_pages
THRIFT=parquet_pages/parquet.thrift
OUT=parquet_pages

.PHONY: gen clean build publish

gen:
	docker build -t thrift-gen .
	docker run --rm -v $(PWD):/app thrift-gen \
		thrift --gen py -out /app/$(OUT) /app/$(THRIFT)

clean:
	rm -rf $(OUT)/parquet build dist *.egg-info

build: clean gen
	python -m build

publish: build
	python -m twine upload dist/*
