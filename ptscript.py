import pandas as pd
import boto3
import io
from datetime import datetime

# Constants
REGION = 'us-east-1'
SOURCE_DB = 'devoted_health_prod'
QUERY_OUTPUT_LOCATION = 's3://zignaai-deidentified-claimsdata/query output/'
WORKGROUP_NAME = 'SelectionQueries-Production'

def run_query(athena_client, s3_client, query_string, source_db, query_output_location, workgroup):
    """
    Execute an Athena query and retrieve the result as a pandas DataFrame.
    
    Args:
        athena_client: boto3 Athena client
        s3_client: boto3 S3 client
        query_string (str): SQL query to execute
        source_db (str): Source database name
        query_output_location (str): S3 location for query output
        workgroup (str): Athena workgroup name
    
    Returns:
        pd.DataFrame or None: Resulting dataframe if successful, None otherwise
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': source_db},
            ResultConfiguration={'OutputLocation': query_output_location},
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']
        
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            query_state = query_status['QueryExecution']['Status']['State']
            if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        
        if query_state == 'SUCCEEDED':
            try:
                query_result_path = query_status['QueryExecution']['ResultConfiguration']['OutputLocation']
                bucket = query_result_path.split('//')[1].split('/')[0]
                key = '/'.join(query_result_path.split('//')[1].split('/')[1:])
                buffer = io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())
                df = pd.read_csv(buffer, dtype=str)
                return df
            except Exception as e:
                print("QUERY EXECUTION SUCCESSFUL BUT GOT ERROR WHILE CONVERSION TO DATAFRAME")
                print(e)
                return None
        else:
            print(f"Query execution failed Status: {query_state}")
            return None
    except Exception as e:
        print("GOT EXCEPTION WHILE RUNNING QUERY")
        print(e)
        return None

def add_proc_code_flag(df, aws_access_key_id, aws_secret_access_key):
    """
    Adds a 'proc_code_flag' column to the input dataframe based on reference data from Athena.
    
    The flag is determined by checking if 'procedure_code' is in 'ref_intersect' or 'target_intersect'
    lists obtained from the reference data:
    - 'reference' if in 'ref_intersect'
    - 'target' if in 'target_intersect'
    - 'other' if in neither
    
    Args:
        df (pd.DataFrame): Input dataframe with required columns:
            ['payer_control_number', 'member_medicare_id', 'service_date', 
             'rendering_provider_npi', 'procedure_code']
        aws_access_key_id (str): AWS access key ID for authentication
        aws_secret_access_key (str): AWS secret access key for authentication
    
    Returns:
        pd.DataFrame: Dataframe with added 'proc_code_flag' column plus 'ref_intersect' and 'target_intersect'
    
    Raises:
        ValueError: If required columns are missing in the input dataframe
        RuntimeError: If fetching reference data from Athena fails
    """
    # Check for required columns
    required_columns = ['payer_control_number', 'member_medicare_id', 'service_date', 
                       'rendering_provider_npi', 'procedure_code']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Calculate lookback date
    current_date = datetime.today().date()
    lookback_date = (current_date - pd.DateOffset(months=18)).replace(day=1).strftime("'%Y-%m-%d'")
    
    # Construct the query string
    query_string = f"""SELECT distinct p1.payer_control_number,
            p1.member_medicare_id,
            p1.rendering_provider_npi,
            p1.service_date,
            array_intersect(
                array_distinct(array_agg(p1.procedure_code)),
                array_distinct(array_agg(column1))
            ) as ref_intersect,
            array_intersect(
                array_distinct(array_agg(p2.procedure_code)),
                array_distinct(array_agg(column2))
            ) as target_intersect
        FROM devoted_health_prod.transformed_claims AS p1
            JOIN devoted_health_prod.transformed_claims AS p2 ON p1.payer_control_number = p2.payer_control_number
            and p1.member_medicare_id = p2.member_medicare_id
            AND p1.rendering_provider_npi = p2.rendering_provider_npi
            AND p1.service_date = p2.service_date
            LEFT JOIN zigna_reference_data.medicare_ncci_ptp_edits on p1.procedure_code = medicare_ncci_ptp_edits.column1
            AND p2.procedure_code = medicare_ncci_ptp_edits.column2
        where p1.bill_type_code is null
            and p1.first_service_date >= effective_date
            and p1.first_service_date < deletion_date
            AND p1.plan_paid_amount > 0
            AND p2.plan_paid_amount >= 74
            AND modifier_filter = '0'
            AND p1.procedure_code <> p2.procedure_code
            AND medicare_ncci_ptp_edits.provider_type in ('practitioner')
            AND p1.payment_effective_date >= date({lookback_date})
            AND p1.is_final_paid_indicator = 1
        group by p1.payer_control_number,
            p1.member_medicare_id,
            p1.service_date,
            p1.rendering_provider_npi
        having cardinality(
                array_intersect(
                    array_distinct(array_agg(p1.procedure_code)),
                    array_distinct(array_agg(column1))
                )
            ) >= 1
            and cardinality(
                array_intersect(
                    array_distinct(array_agg(p2.procedure_code)),
                    array_distinct(array_agg(column2))
                )
            ) >= 1"""
    
    # Initialize boto3 clients with provided AWS credentials
    s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    athena_client = boto3.client('athena', region_name=REGION, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    # Fetch reference data
    referance_data = run_query(athena_client, s3_client, query_string, SOURCE_DB, 
                              QUERY_OUTPUT_LOCATION, WORKGROUP_NAME)
    
    if referance_data is None:
        raise RuntimeError("Failed to fetch reference data from Athena")
    
    # Clean reference data
    df_target_ref_clean = referance_data.copy()
    df_target_ref_clean['ref_intersect'] = df_target_ref_clean['ref_intersect'].str.replace('[', '').str.replace(']', '')
    df_target_ref_clean['ref_intersect'] = df_target_ref_clean['ref_intersect'].str.split(',')
    df_target_ref_clean['ref_intersect'] = df_target_ref_clean['ref_intersect'].apply(lambda x: [i.strip() for i in x])
    
    df_target_ref_clean['target_intersect'] = df_target_ref_clean['target_intersect'].str.replace('[', '').str.replace(']', '')
    df_target_ref_clean['target_intersect'] = df_target_ref_clean['target_intersect'].str.split(',')
    df_target_ref_clean['target_intersect'] = df_target_ref_clean['target_intersect'].apply(lambda x: [i.strip() for i in x])
    
    # Merge with input dataframe
    main_df_with_target_ref_codes = df.merge(df_target_ref_clean, 
                                            on=['payer_control_number', 'member_medicare_id', 
                                                'service_date', 'rendering_provider_npi'],
                                            how='left')
    
    # Handle NaN values
    main_df_with_target_ref_codes['target_intersect'] = main_df_with_target_ref_codes['target_intersect'].apply(lambda x: [''] if pd.isna(x) else x)
    main_df_with_target_ref_codes['ref_intersect'] = main_df_with_target_ref_codes['ref_intersect'].apply(lambda x: [''] if pd.isna(x) else x)
    
    # Define flagging function
    def flag_row(row):
        if row['procedure_code'] in row['ref_intersect']:
            return 'reference'
        elif row['procedure_code'] in row['target_intersect']:
            return 'target'
        else:
            return 'other'
    
    # Apply flagging
    main_df_with_target_ref_codes['proc_code_flag'] = main_df_with_target_ref_codes.apply(flag_row, axis=1)
    
    return main_df_with_target_ref_codes