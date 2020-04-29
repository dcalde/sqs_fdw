# Amazon SQS Foreign Data Wrapper for PostgreSQL

This Foreign Data Wrapper (FDW) allows to receive messages from and send to Amazon Simple Message Service (SQS) via SELECT and INSERT.

## Dependencies

This FDW requires the [Multicorn extension](https://multicorn.org/#installation) to be installed.

```shell script
pip install pgxnclient
pgxn install multicorn
```

## Installation

```shell script
python setup.py install
```

```postgresql
CREATE EXTENSION IF NOT EXISTS multicorn;

CREATE SERVER IF NOT EXISTS multicorn_sqs 
    FOREIGN DATA WRAPPER multicorn
OPTIONS (
    wrapper 'sqs_fdw.SQSForeignDataWrapper'
);

CREATE FOREIGN TABLE multicorn_test (
    message_id uuid,
    test character varying,
    test2 int,
    message_attributes jsonb
) SERVER multicorn_sqs OPTIONS (
    aws_access_key_id '',
    aws_secret_access_key '',
    aws_region 'ap-southeast-2',
    queue_url 'https://sqs.*',
    message_attributes 'foo,bar'
);
```

## Usage

```postgresql
INSERT INTO multicorn_test (test, test2)
VALUES ('a', 1), ('b', 2);

SELECT * FROM multicorn_test;
```
