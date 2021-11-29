from google.cloud.bigquery import SchemaField


schema = [
    SchemaField(
        name='repository',
        field_type='STRING',
        description='Repository of SQL that was run'
    ),
    SchemaField(
        name='log_category',
        field_type='STRING',
        description='Summary name of the log'
    ),
    SchemaField(
        name='message',
        field_type='STRING',
        description='Message associated with the log'
    ),
    SchemaField(
        name='run_id',
        field_type='STRING',
        description='Unique identifier for this run'
    ),
    SchemaField(
        name='write_time',
        field_type='STRING',
        description='Time that this row of data was written to BigQuery'
    )
]
