import json
import apache_beam as beam
import time
from datetime import date
import argparse

from apache_beam.io.filesystem import CompressionTypes


def convert_string_to_time(time_string):
    return time.strptime(time_string, '%Y-%m-%d %H:%M:%S %Z')


def split_and_enrich_with_time(line):
    line_split = line.rstrip().split(",")
    transaction_time = convert_string_to_time(line_split[0])
    return transaction_time, float(line_split[3])


def filter_transaction(transaction, app_params):
    return transaction[1] > app_params.get('min_amount') and transaction[0].tm_year != app_params.get('year_to_remove')


def check_args(app_args):
    if app_args.year is None or (2000 <= int(app_args.year) >= date.today().year):
        print("Parameter year accepted between 2000 and", date.today().year)
        exit(-1)
    if app_args.min_amount is None or float(app_args.min_amount) < 0:
        print("Parameter min_amount>=0")
        exit(-1)
    if app_args.input_file is None:
        print("Parameter --input_file is required")
        exit(-1)
    return {
        "year_to_remove": int(app_args.year),
        "min_amount": float(app_args.min_amount),
        "input_file": app_args.input_file
    }


parser = argparse.ArgumentParser()
parser.add_argument('--year', default=2010, help='Year to not consider format like 2010')
parser.add_argument('--min_amount', default=10, help='Minimum transaction amount')
parser.add_argument('--input_file', help='Csv transaction file url')
args = parser.parse_args()
app_param = check_args(args)


# Running locally in the DirectRunner.
with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read lines' >> beam.io.ReadFromText(app_param.get("input_file"), skip_header_lines=1)
        | 'Split lines' >> beam.Map(lambda line: split_and_enrich_with_time(line))
        | 'Filter lines' >> beam.Filter(lambda line: filter_transaction(line, app_param))
        | 'Create the pair' >> beam.Map(lambda transaction: (time.strftime("%Y-%m-%d", transaction[0]), transaction[1]))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Create json' >> beam.Map(lambda sum_per_day: json.dumps({'date': sum_per_day[0], 'total_amount': sum_per_day[1]}))
        | 'Write results' >> beam.io.WriteToText('output/result', file_name_suffix='.jsonl.gz', compression_type=CompressionTypes.GZIP)
    )
