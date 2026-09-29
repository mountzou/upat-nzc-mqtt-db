"""Disposable API preview: force every PostgreSQL connection read-only and bounded."""
import psycopg2
import sys
import uvicorn

sys.path.insert(0, '/app')

original_connect = psycopg2.connect


def readonly_connect(*args, **kwargs):
    kwargs['options'] = kwargs.get('options', '') + (
        ' -c default_transaction_read_only=on -c statement_timeout=60000'
        ' -c lock_timeout=2000 -c idle_in_transaction_session_timeout=60000'
        ' -c max_parallel_workers_per_gather=0 -c work_mem=16MB'
        ' -c application_name=shelly_stage2_readonly_preview'
    )
    return original_connect(*args, **kwargs)


psycopg2.connect = readonly_connect
import database
with database.get_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute('SHOW transaction_read_only')
        assert next(iter(cursor.fetchone().values())) == 'on'
        cursor.execute('SHOW statement_timeout')
        assert next(iter(cursor.fetchone().values())) == '1min'
print('Readonly preview database guards verified', flush=True)
uvicorn.run('main:app', host='0.0.0.0', port=8000, workers=1)
