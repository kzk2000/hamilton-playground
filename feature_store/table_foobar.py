from feature_store.hamilton_table import HamiltonTable

table_obj = HamiltonTable(
    schema='schema',
    table_name='table_foobar',
    nodes=['foo', 'bar', 'foobar']
)

table_obj.run()
