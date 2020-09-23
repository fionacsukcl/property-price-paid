# property-price-paid

## Overview
Pipeline to extract [property transaction data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) from csv files. 

The files do not include any identifier for properties, so `ParsePropertyTransactionsFn` generates an identifier from the data we have using the postcode, PAON, SAON and street of the address. The identifier will enable us to group on properties where more than one transaction has occurred. 

`hashlib` has been used for generating the identifer rather than `hash` to guarantee repeatability, as in later versions of Python, `PYTHONHASHSEED=random` is on by default meaning the hash returned is different each time the pipeline is run. 

The files also do not contain any headings, each of the column values are mapped to the column name.

The `PCollection` is then mapped to key-value pairs with the `propertyHash` as the key, so that `GroupByKey()` can be applied which results in any transaction with the same property hash to be grouped together.

## Running the pipeline in the cloud
In the `property_price_paid.py` file, there are arguments that can be changed in the `run` function so that the solution is run in the cloud:

      # CHANGE 1/6: The Google Cloud Storage path is required
      # for outputting the results.
      default='output_' + datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + '.json'

      # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',

      # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--project=SET_YOUR_PROJECT_ID_HERE',

      # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
      # is required in order to run your pipeline on the Google Cloud
      # Dataflow Service.
      '--region=SET_REGION_HERE',

      # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',

      # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',

## Default input file
The default file `pp-monthly-small.csv` is a modified file used for testing purposes. It is a small subset of the original monthly file available on [gov.uk](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads), with some additional records manually added to demonstrate the grouping by property.

## Next steps

### Resilience
Error handling would be beneficial to allow the pipeline to fail gracefully should there be issues. For example, the input file could contain badly formatted records which the current implementation does not handle well.

### Parameterisation
The pipeline arguments could be parameterised to make it more flexible, e.g. the input, output and pipeline runner arguments can be parameterised in a config file to handle different types of runs without having to modify the main pipeline file.

### Tests
Unit tests can be added at each of the key pipeline stages to ensure transforms are applied as expected. Examples of tests that could be created include:
* `PropertyTransactions` to ensure that the hash generates an expected value. If the hash mechanism has changed then that means the properties cannot be matched on historical pipeline runs.
* `propertyHash` to ensure that the format is correct.
* `GroupByPropertyHash` to ensure that the relevant transactions are grouped together for the same property.
* `ConvertToJson` to ensure that valid JSON is produced.

This will ensure that any subsequent changes made to the pipeline do not break the current implementation.

### Implementation

#### Identifying unique properties

Depending on how the data is to be analysed, the current way of identifying unique properties can be limiting. For example, the current implementation does not take into account any changes to the property that could impact the price such as a semi-detached property merging with the adjacent property.

Over time, there could also be changes to the address details of the same property in the input files, either caused by the data provided or genuine changes to an address, in which case the hashing would fail to recognise the same property unless we are able to supplement the data with additional information that can help us identify when the property details have changed.

#### Subsequent runs

Additional steps would need to be included to handle any subsequent runs for additional data files. At the moment, transactions are grouped in isolation of what is available in the file, but a transaction for the same property could be available in a different file.

#### Runner compatibility

There are compatibility issues with the ConvertToJson step in the pipeline when it is being run using DataflowRunner.