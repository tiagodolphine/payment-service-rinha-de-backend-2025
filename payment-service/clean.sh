#!/bin/sh

curl --location --request POST 'localhost:8001/admin/purge-payments' \
--header 'X-Rinha-Token: 123'

curl --location --request POST 'localhost:8002/admin/purge-payments' \
--header 'X-Rinha-Token: 123'