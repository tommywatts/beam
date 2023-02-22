import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipelines.beam_transactions_composite import CsvToRows, CompositeTransform


def test_csv_to_rows():
    test_input = [
        "2000-01-01 00:00:00 UTC,wallet00000abc123,wallet00000def456,100",
        "2010-01-01 12:00:00 UTC,wallet00000abc123,wallet00000def456,50",
    ]

    expected_output = [
        beam.Row(timestamp=datetime.datetime(2000, 1, 1, 0, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=100.0),
        beam.Row(timestamp=datetime.datetime(2010, 1, 1, 12, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=50.0),
    ]

    with TestPipeline() as p:
        rows = p | beam.Create(test_input) | beam.ParDo(CsvToRows())

        assert_that(rows, equal_to(expected_output))


def test_composite_transform():

    test_input = [
        beam.Row(timestamp=datetime.datetime(2000, 1, 1, 0, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=100.0),
        beam.Row(timestamp=datetime.datetime(2010, 1, 1, 12, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=50.0),
        beam.Row(timestamp=datetime.datetime(2020, 1, 1, 0, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=100.0),
        beam.Row(timestamp=datetime.datetime(2020, 1, 1, 12, 0), origin="wallet00000abc123", destination="wallet00000def456", transaction_amount=50.0),
    ]


    expected_output = ['2010-01-01,50.0', '2020-01-01,150.0']

    with TestPipeline() as p:
        rows = p | beam.Create(test_input) | CompositeTransform() | beam.ToString.Kvs()

        assert_that(rows, equal_to(expected_output))
