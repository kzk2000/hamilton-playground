from feature_store.hamilton_table import HamiltonTable

table_obj = HamiltonTable(
    schema='schemaA',
    table_name='table_bar',
    nodes=['bar']
)

table_obj.run()
