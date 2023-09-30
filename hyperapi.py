'''
from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'WorldIndicators.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")
        connection.catalog.create_schema('Extract')
        example_table = TableDefinition(TableName('Extract','Extract'), [
            TableDefinition.Column('rowID', SqlType.big_int()),
            TableDefinition.Column('value', SqlType.big_int()),
         ])
        print("The table is defined.")
        connection.catalog.create_table(example_table)
        with Inserter(connection, example_table) as inserter:
            for i in range (1, 101):
                inserter.add_row(
                    [ i, i ]
            )
            inserter.execute()
        print("The data was added to the table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
'''
from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName

with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")
    with Connection(hyper.endpoint, 'WorldIndicators.hyper', CreateMode.NONE) as connection:
        print("The connection to the .hyper file is open.")
        with Inserter(connection, TableName('Extract','Extract')) as inserter:
            inserter.add_row([101, 101])
            inserter.add_row([102, 102])
            inserter.add_row([103, 104])
            inserter.execute()
        print("The data in table \"Extract\" has been updated.")
    print("The connection to the Hyper file is closed.")
print("The HyperProcess has shutdown.")



# with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # print("The HyperProcess has started.")
    # with Connection(hyper.endpoint, 'WorldIndicators.hyper', CreateMode.NONE) as connection:
