#!/bin/bash

gsutil cp gs://de-proj-1/scripts/sales_data_etl.tar.gz /tmp/
tar -xvzf /tmp/sales_data_etl.tar.gz -C /tmp/