"""
Pipeline to extract property price paid data in csv format and covert to JSON. 
Each of the transactions are grouped by the property.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://property-price-paid/pp-monthly-small.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='output_' + datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + '.json',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    (
      p
      | 'ReadInputFile' >> ReadFromText(known_args.input)
      | 'WriteOutput' >> WriteToText(known_args.output)
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()