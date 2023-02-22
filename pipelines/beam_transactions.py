import argparse
import logging
from datetime import datetime
import csv
import jsons

import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

DATE_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


class CsvToRows(beam.DoFn):
    @staticmethod
    def _parse_date(date, date_format=DATE_FORMAT):
        return datetime.strptime(date, date_format)

    def process(self, line):
        row = next(csv.reader([line]))
        yield beam.Row(
            timestamp=self._parse_date(row[0]),
            origin=str(row[1]),
            destination=str(row[2]),
            transaction_amount=float(row[3]),
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        dest="input",
        required=True,
        help="Input file to process.",
    )

    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="Output file to write results to.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        aggregated = (
            p
            | "Read" >> ReadFromText(known_args.input, skip_header_lines=True)
            | "To Rows" >> beam.ParDo(CsvToRows())
            | "transaction_amount > 20" >> beam.Filter(lambda row: row.transaction_amount > 20.0)
            | "timestamp > 2010" >> beam.Filter(lambda row: row.timestamp.year >= 2010)
            | "Group by date" >> beam.GroupBy(date=lambda row: str(row.timestamp.date())).aggregate_field(
                "transaction_amount", sum, "total_amount"
            )
        )

        aggregated | beam.ToString.Kvs() | "Write to csv" >> WriteToText(
            known_args.output,
            file_name_suffix=".csv",
            num_shards=1,
            shard_name_template="",
            header="date, total_amount",
        )

        aggregated | beam.Map(jsons.dumps) | "Write to jsonl" >> WriteToText(
            known_args.output, file_name_suffix=".jsonl.gz", shard_name_template=""
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
