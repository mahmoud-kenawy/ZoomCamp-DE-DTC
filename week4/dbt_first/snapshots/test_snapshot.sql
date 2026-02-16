{% snapshot test_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='country',
        strategy='check',
        check_cols=['country', 'iso_codes']
    )
}}
SELECT * FROM schema_db.countrycode
{% endsnapshot %}