from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal
import pandas as pd
import tableauserverclient as TSC
import boto3

def GetDataFromS3Bucket(s3BucketName):
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=s3BucketName)
    for obj in objects['Contents']:
        #print(obj)
        if ".csv" in obj['Key']: 
            data=s3_client.get_object(Bucket=s3BucketName,Key=obj.get('Key'))
            #print(data)
            fileName=obj['Key'].split('/')[1]
            print(fileName)
            fileContent=data.get('Body').read().decode('utf-8').split("\r\n")  
            with open(fileName,'w') as f:
                for n in fileContent:
                    if len(n)>2: ### avoid loding blank line
                        f.write(n+u"\n")

    return fileName  
        
def GetDataSchemaFromFile(fileName):
    df = pd.read_csv(fileName)
    df = df.convert_dtypes()
    Columns=[]
    for col,dtype in df.dtypes.to_dict().items():
        # if dtype=='Float64':
            # Columns.append(TableDefinition.Column(col,SqlType.double()))
        # elif dtype=='Int64':
            # Columns.append(TableDefinition.Column(col,SqlType.int()))
        # else:
            Columns.append(TableDefinition.Column(col,SqlType.text()))          
    return Columns



def CreateHyperExtractForFile(fileName,schemaName,hyperFileName):
    # Step 1: Start a new private local Hyper instance
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp' ) as hyper:

    # Step 2:  Create the the .hyper file, replace it if it already exists
        with Connection(endpoint=hyper.endpoint, 
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        database=hyperFileName) as connection:

    # Step 3: Create the schema
            connection.catalog.create_schema(schemaName)

    # Step 4: Create the table definition
            Columns=GetDataSchemaFromFile(fileName)
            table_name=fileName.split("\\")[-1]#.replace(".","_")
            #print(table_name)
            schema = TableDefinition(table_name=TableName(schemaName,table_name),columns=Columns)
    # Step 5: Create the table in the connection catalog
            connection.catalog.create_table(schema)
        
            insert_csv_data = connection.execute_command(
                command=f"COPY {schema.table_name} FROM {escape_string_literal(fileName)} WITH "
                f"(format csv, NULL 'NULL', delimiter ',', header)"
            )
        print("\nHyper File Created......")
    
    return hyperFileName


def uploadHyperFileToTableauServer(hyper_file_path):
    
    ###Config part optimize later
    server_url = 'https://prod-useast-a.online.tableau.com/'
    site_id= 'useready'  # Optional, if using a specific site
    mytoken_name='restapi'
    mytoken_secret='serret'
    project_id = 'id'
    ###Config part optimize later  
    
    # Create a server object and establish the connection
    server = TSC.Server(server_url, use_server_version=True)
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name=mytoken_name, personal_access_token=mytoken_secret, site_id=site_id) 
 
    with server.auth.sign_in_with_personal_access_token(tableau_auth):
        print('\n\n[Logged in successfully to {}]'.format(server_url))  
        new_datasource = TSC.DatasourceItem(project_id)
        print('\n\nLoading Hyper File   <',hyper_file_path,'>')
        uploadFile = server.datasources.publish(new_datasource, hyper_file_path, 'Overwrite')
        print('\nUploaded Successfully....\n')




################## Main Function #######################

fileName=GetDataFromS3Bucket('nomurakk')#,'schemadisco')#/electronic-card-transactions-april-2023-csv-tables.csv')
#exit(0)
PATH_TO_CSV= "C:\Projects -Useready\CitiBank\hyperapi\\" + fileName  #sample_csv_for_hyper.csv
PATH_TO_HYPER =PATH_TO_CSV.split(".")[0]+".hyper"
print(PATH_TO_HYPER)

hyper_file_path =CreateHyperExtractForFile(PATH_TO_CSV,'Extract',PATH_TO_HYPER)
uploadHyperFileToTableauServer(hyper_file_path)
