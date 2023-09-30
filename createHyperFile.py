from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal


PATH_TO_CSV = 'sample_csv_for_hyper.csv'
PATH_TO_HYPER = 'test_hyper_extract_api.hyper'


# Step 1: Start a new private local Hyper instance
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp' ) as hyper:

# Step 2:  Create the the .hyper file, replace it if it already exists
    with Connection(endpoint=hyper.endpoint, 
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                    database=PATH_TO_HYPER) as connection:

# Step 3: Create the schema
        connection.catalog.create_schema('Extract')

# Step 4: Create the table definition
        # schema = TableDefinition(table_name=TableName('Extract','Extract'),
            # columns=[
            # TableDefinition.Column('date', SqlType.date()),
            # TableDefinition.Column('country', SqlType.text()),
            # TableDefinition.Column('sales', SqlType.double()),
            # TableDefinition.Column('profit', SqlType.double()),
            # TableDefinition.Column('profit_ratio', SqlType.double()),
         # ])
    
    
        ### Added by Kamlesh to make it dynamic
        import pandas as pd

        df = pd.read_csv(PATH_TO_CSV)
        df = df.convert_dtypes()
        Columns=[]
        for col,dtype in df.dtypes.to_dict().items():
            #dtype=str(dtype).replace('Float64','double').replace('Int64','integer')
            if dtype=='Float64':
                Columns.append(TableDefinition.Column(col,SqlType.double()))
            elif dtype=='Int64':
                Columns.append(TableDefinition.Column(col,SqlType.int()))
            else:
                Columns.append(TableDefinition.Column(col,SqlType.text()))
              
            #Columns+="TableDefinition.Column("+col+"',SqlType."+dtype+"()),"
        #Columns=str(Columns).replace('"','')
        print(Columns)

        
        schema = TableDefinition(table_name=TableName('Extract','Extract'),columns=Columns)
# Step 5: Create the table in the connection catalog
        connection.catalog.create_table(schema)
    
        insert_csv_data = connection.execute_command(
            command=f"COPY {schema.table_name} FROM {escape_string_literal(PATH_TO_CSV)} WITH "
            f"(format csv, NULL 'NULL', delimiter ',', header)"
        )

    print("The connection to the Hyper file is closed.")