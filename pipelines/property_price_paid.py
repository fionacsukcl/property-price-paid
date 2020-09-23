"""
Pipeline to extract property price paid data in csv format and covert to JSON. Each of the transactions are grouped by the property.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import csv
import hashlib
import logging
import re

from past.builtins import unicode
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class ParsePropertyTransactionsFn(beam.DoFn):
  '''
  Parse each element in the input PCollection.

  Column header of file: 
  transactionId, price, dateOfTransfer, postcode, propertyType, oldNew, duration, PAON, SAON, street, locality, townCity, district, county, ppdCategoryType, recordStatus
  '''

  def __init__(self):
    super(ParsePropertyTransactionsFn, self).__init__()

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]

      #The purpose of the property hash is to uniquely identify the same property that can appear in multiple transaction records. The hash is performed on the postcode, PAON, SAON and street name of the address.
      propertyHashValues = (row[3] + '|' + row[7] + '|' + row[8] + '|' + row[9]).encode('utf-8')
      propertyHash = hashlib.sha256(propertyHashValues).hexdigest()

      #Map each of the columns to a column header based on the columns stated here: https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd with the addition of the 'propertyHash' that has been generated above.
      yield {
        'propertyHash': propertyHash,
        'transactionId': row[0],
        'price': row[1],
        'dateOfTransfer': row[2],
        'postcode': row[3],
        'propertyType': row[4],
        'oldNew': row[5],
        'duration': row[6],
        'PAON': row[7],
        'SAON': row[8],
        'street': row[9],
        'locality': row[10],
        'townCity': row[11],
        'district': row[12],
        'county': row[13],
        'ppdCategoryType': row[14],
        'recordStatus': row[15]
      }
    except: 
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)

class PropertyTransactions(beam.PTransform):
  def expand(self, pcoll):
    return (
      pcoll
      | 'ParsePropertyTransactionsFn' >> beam.ParDo(ParsePropertyTransactionsFn())
    )

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
      | 'PropertyTransactions' >> PropertyTransactions()
      | 'WriteOutput' >> WriteToText(known_args.output)
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()