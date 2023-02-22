gcloud_transactions_sample=gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv
output_location=output/results
test_image_tag=test_beam:latest

run-direct:
	python pipelines/beam_transactions.py \
		--runner=DirectRunner \
		--input $(gcloud_transactions_sample) \
		--output $(output_location)

run-direct-composite:
	python pipelines/beam_transactions_composite.py \
		--runner=DirectRunner \
		--input $(gcloud_transactions_sample) \
		--output $(output_location)

test-unit:
	python -m pytest tests

docker-build:
	docker build . -f Dockerfile -t $(test_image_tag)

docker-test-unit: docker-build
	docker run $(test_image_tag) bash -c "python -m pytest tests"
