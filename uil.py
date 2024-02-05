from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from datetime import datetime, timedelta
import boto3 , botocore
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, concat, lit, from_json ,  explode, concat_ws, explode_outer, col,split,udf, from_utc_timestamp, get_json_object, when , coalesce , to_date, current_date , substring
from pyspark.sql.types import StringType, StructType , StructField, DoubleType, TimestampType, DecimalType, BooleanType , ArrayType
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook





# Access connection details
key_id = Variable.get("AWS_ACCESS_KEY_ID")
secret_key_id = Variable.get("AWS_SECRET_ACCESS_KEY")
bucket_name="training-pyspark"
s3= boto3.resource('s3',aws_access_key_id=key_id, aws_secret_access_key=secret_key_id)

today = datetime.now()
# Get yesterday's date
yesterday = today - timedelta(days=1)
# Format dates as strings in "YYYY_MM_dd" format
today_str = today.strftime("%Y_%m_%d")
yesterday_str = yesterday.strftime("%Y_%m_%d")

# Define S3 bucket and folder details
s3_folder = 'drop'
file_pattern = 'uil_'+today_str 

current_date = datetime.now().strftime("%Y_%m_%d")
print(current_date)
today_file = 'uil_'+str(current_date)
current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
previous_day = yesterday.strftime("%Y_%m_%d")
print(previous_day)
yesterday_file = 'uil_'+str(previous_day)

key = f"drop/{today_file}.parquet"
key2 = f"drop/{yesterday_file}.parquet"



# Create Spark session
'''
spark = SparkSession.builder \
    .appName("account_portfolio_uil") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()
'''
spark = SparkSession.builder \
    .appName("account_portfolio_uil") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.parquet.columnarReaderBatchSize", "2048") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.sources.bucketing.enabled", "false") \
    .getOrCreate()

# Configure AWS credentials in Spark session

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key_id)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'account_portfolio_uil',
    default_args=default_args,
    description='DAG for processing S3 objects',
    schedule_interval=None,
)


def watch_file_drop():
    try:
        s3.Object(bucket_name, key).load()
        print(f"File {file_pattern}.parquet exists")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("file does not exist")
            return False
        else:
            return f"error : {e}"

def validation_schema():
    print("before load")
    today_df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    yesterday_df = spark.read.parquet(f"s3a://{bucket_name}/{key2}", header=True, inferSchema=True)
    today_schema = today_df.schema
    yesterday_schema = yesterday_df.schema
    

    if today_schema == yesterday_schema:
        print("The schemas are identical.")
        return True
    else:
        print("The schemas are different.")
        print("Schema for day1:")
        print(today_schema)
        print("Schema for day2:")
        print(yesterday_schema)
        return False


def loan_debt_sale_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None
    
    uil_df = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df = uil_df.filter(uil_df.dark == False)
    uil_df = uil_df.filter(uil_df.sale_info != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df = uil_df.withColumn(col, get_json_object(uil_df.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df.portfolios_created_at.desc())
    uil_df = uil_df.withColumn("rownum", row_number().over(window))
    uil_df = uil_df.filter(uil_df.rownum == 1)

    uil_df = uil_df.withColumn("loan_id", uil_df.loan_id.cast("int"))

    uil_df = uil_df.withColumn('temp_explode_sale_info', conversion_udf(uil_df.sale_info))
    uil_df = uil_df.select("*", explode(uil_df.temp_explode_sale_info).alias("explode_sale_info"))

    list_of_attributes_sale_info = ['uuid', 'sale_transaction_timestamp', 'sale_date', 'debt_buyer', 'loan_task_id',
                                    'percentage_sold', 'sale_amount', 'sale_fees_amount', 'sale_interest_amount',
                                    'sale_principal_amount', 'debt_sale_file_id', 'debt_sale_file_date',
                                    'debt_sale_file_uuid', 'rebuy_date', 'rebuy_amount', 'rebuy_reason',
                                    'rebuy_file_uuid', 'rebuy_transaction_timestamp']

    for col in list_of_attributes_sale_info:
        uil_df = uil_df.withColumn(col, get_json_object(uil_df.explode_sale_info, f"$.{col}"))

    uil_df = uil_df \
        .withColumn("created_time", from_utc_timestamp(uil_df.sale_transaction_timestamp, 'CST')) \
        .withColumn("rebuy_created_time", from_utc_timestamp(uil_df.rebuy_transaction_timestamp, 'CST')) \
        .withColumn("sale_date", uil_df.sale_date.cast("date")) \
        .withColumn("debt_sale_file_date", uil_df.debt_sale_file_date.cast("date")) \
        .withColumn("rebuy_date", uil_df.rebuy_date.cast("date")) \
        .withColumn("debt_buyer_name", uil_df.debt_buyer) \
        .withColumn("loan_task_id", uil_df.loan_task_id.cast("int")) \
        .withColumn("debt_sale_file_id", uil_df.debt_sale_file_id.cast("int")) \
        .withColumn("percentage_sold", uil_df.percentage_sold.cast('decimal(10,2)')) \
        .withColumn("sale_amount", uil_df.sale_amount.cast('decimal(10,2)')) \
        .withColumn("sale_fees_amount", uil_df.sale_fees_amount.cast('decimal(10,2)')) \
        .withColumn("sale_interest_amount", uil_df.sale_interest_amount.cast('decimal(10,2)')) \
        .withColumn("sale_principal_amount", uil_df.sale_principal_amount.cast('decimal(10,2)')) \
        .withColumn("rebuy_amount", uil_df.rebuy_amount.cast('decimal(10,2)')) \
        .withColumnRenamed("uuid", "loan_debt_sale_uuid")

    final_cols = ['loan_id', 'loan_uuid', 'loan_debt_sale_uuid', 'created_time', 'sale_date', 'debt_buyer_name',
                  'loan_task_id', 'percentage_sold', 'sale_amount', 'sale_fees_amount', 'sale_interest_amount',
                  'sale_principal_amount', 'debt_sale_file_id', 'debt_sale_file_date', 'debt_sale_file_uuid',
                  'rebuy_date', 'rebuy_amount', 'rebuy_reason', 'rebuy_file_uuid', 'rebuy_created_time',
                  'etl_ingestion_time', 'source_file_creation_time']

    loan_debt_sale_df = uil_df.select(final_cols)
    
    # Print or show the DataFrame
    #loan_debt_sale_df.limit(2).show(truncate=False)  
    print("loan_debt_sale_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_debt_sale_df.collect()]
    
    return serialized_data

def loan_servicing_settlements_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df1 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df1 = uil_df1.filter(uil_df1.dark == False)

    uil_df1 = uil_df1.filter(uil_df1.settlements != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df1 = uil_df1.withColumn(col, get_json_object(uil_df1.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df1.portfolios_created_at.desc())
    uil_df1 = uil_df1.withColumn("rownum", row_number().over(window))
    uil_df1 = uil_df1.filter(uil_df1.rownum == 1)

    uil_df1 = uil_df1.withColumn("loan_id", uil_df1.loan_id.cast("int"))

    list_of_attributes_customer_information = ['customer_id']
    for col in list_of_attributes_customer_information:
        uil_df1 = uil_df1.withColumn(col, get_json_object(uil_df1.customer_information, f"$.{col}"))

    #explode settlements
    uil_df1 = uil_df1.withColumn('temp_explode_settlements',conversion_udf(uil_df1.settlements))
    uil_df1 = uil_df1.select("*",explode(uil_df1.temp_explode_settlements).alias("explode_settlements"))

    list_of_attributes_settlements = ['amount','servicing_settlement_offer_id','status', 'end_date', 'start_date', 'payment_method', 'settlement_type',
                                      'number_of_payments', 'transaction_timestamp', 'id', 'uuid', 'lock_in_date',
                                      'contract_id']

    for col in list_of_attributes_settlements:
        uil_df1 = uil_df1.withColumn(col, get_json_object(uil_df1.explode_settlements, f"$.{col}"))

    uil_df1 = uil_df1\
    .withColumn("total_amount", uil_df1.amount.cast('decimal(10,2)'))\
    .withColumn("created_time", from_utc_timestamp(uil_df1.transaction_timestamp, 'CST'))\
    .withColumn("structure", uil_df1.settlement_type)\
    .withColumn("end_date", uil_df1.end_date.cast("date"))\
    .withColumn("payment_count", uil_df1.number_of_payments.cast("int"))\
    .withColumn("customer_id", uil_df1.customer_id.cast("int")) \
    .withColumnRenamed("uuid", "loan_servicing_settlement_uuid") \
    .withColumn("loan_servicing_settlement_id", uil_df1.id.cast('int')) \
    .withColumn("lock_in_date", uil_df1.lock_in_date.cast("date")) \
    .withColumn("contract_id", uil_df1.contract_id.cast("int")) \
    .withColumn("start_date", uil_df1.start_date.cast("date"))\
    .withColumn("servicing_settlement_offer_id", uil_df1.servicing_settlement_offer_id.cast("int"))


    #explode settlement_offers
    offer_df = uil_df1.select(["loan_servicing_settlement_id", "settlement_offers"])
    offer_df = offer_df.filter(offer_df.settlement_offers != '[]')
    offer_df = offer_df.withColumn('temp_explode_settlement_offers', conversion_udf(uil_df1.settlement_offers))
    offer_df = offer_df.select("*", explode(offer_df.temp_explode_settlement_offers).alias("explode_settlement_offers"))

    list_of_attributes_settlement_offers = ['id', 'status', 'offered_at', 'viewed_at', 'offer_expiration_date',
                                            'settlement_type', 'customer_using_dmc', 'first_payment_date',
                                            'number_of_payments', 'amount_of_settlement']

    for col in list_of_attributes_settlement_offers:
        offer_df = offer_df.withColumn(col, get_json_object(offer_df.explode_settlement_offers, f"$.{col}"))

    offer_df = offer_df \
        .withColumnRenamed("status", "offer_status") \
        .withColumn("offer_created_time", from_utc_timestamp(offer_df.offered_at, 'CST')) \
        .withColumn("offer_viewed_time", from_utc_timestamp(offer_df.viewed_at, 'CST')) \
        .withColumn("offer_expiration_date", offer_df.offer_expiration_date.cast("date")) \
        .withColumn("offer_structure", offer_df.settlement_type) \
        .withColumn("customer_using_dmc_flag", offer_df.customer_using_dmc.cast(BooleanType())) \
        .withColumn("offer_first_payment_date", offer_df.first_payment_date.cast("date")) \
        .withColumn("offer_payment_count", offer_df.number_of_payments.cast("int")) \
        .withColumn("offer_total_amount", offer_df.amount_of_settlement.cast('decimal(10,2)')) \
        .withColumnRenamed("id", "settlement_offers_id")

    offer_df = offer_df.withColumn("settlement_offers_id", offer_df.settlement_offers_id.cast('int'))

    # Left join settlement_offer record to settlement record
    uil_df1 = uil_df1.join(offer_df, ((uil_df1.servicing_settlement_offer_id == offer_df.settlement_offers_id) &
                                    (uil_df1.loan_servicing_settlement_id == offer_df.loan_servicing_settlement_id)),
                          "left").drop(offer_df.loan_servicing_settlement_id)

    final_cols = ['loan_id','loan_uuid','customer_id','total_amount','status','end_date','start_date',
                  'payment_method','structure','payment_count','created_time','servicing_settlement_offer_id',
                  'loan_servicing_settlement_id','loan_servicing_settlement_uuid','lock_in_date','contract_id',
                  'offer_status','offer_created_time','offer_viewed_time','offer_expiration_date','offer_structure',
                  'customer_using_dmc_flag','offer_first_payment_date','offer_payment_count','offer_total_amount',
                  'etl_ingestion_time','source_file_creation_time']

    loan_servicing_settlement_df = uil_df1.select(final_cols)
    
    # Print or show the DataFrame
    #loan_servicing_settlement_df.limit(2).show(truncate=False)
    print("loan_servicing_settlements_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_servicing_settlement_df.collect()]
    
    return serialized_data


def active_installments_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df2 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df2 = uil_df2.filter(uil_df2.dark == False)
    uil_df2 = uil_df2.filter(uil_df2.installments != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df2 = uil_df2.withColumn(col, get_json_object(uil_df2.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df2.portfolios_created_at.desc())
    uil_df2 = uil_df2.withColumn("rownum", row_number().over(window))
    uil_df2 = uil_df2.filter(uil_df2.rownum == 1)

    uil_df2 = uil_df2.withColumn('temp_explode_installments',conversion_udf(uil_df2.installments))
    uil_df2 = uil_df2.select("*",explode(uil_df2.temp_explode_installments).alias("explode_installments"))

    list_of_attributes_active_installments = ['id', 'amount', 'interest_amount', 'principal_amount', 'late_fees_amount',
                                              'adjusted_effective_date','original_effective_date']
    for col in list_of_attributes_active_installments:
        uil_df2 = uil_df2.withColumn(col, get_json_object(uil_df2.explode_installments, f"$.{col}"))

    uil_df2 = uil_df2\
    .withColumn("active_installment_uuid",  sha1(concat(uil_df2.id)))\
    .withColumn("loan_id", uil_df2.loan_id.cast("long"))\
    .withColumn("active_installment_id", uil_df2.id.cast("int"))\
    .withColumn("adjusted_effective_date", uil_df2.adjusted_effective_date.cast("date"))\
    .withColumn("original_effective_date", uil_df2.original_effective_date.cast("date"))\
    .withColumn("interest_amount", uil_df2.interest_amount.cast("decimal(10,2)"))\
    .withColumn("principal_amount", uil_df2.principal_amount.cast("decimal(10,2)"))\
    .withColumn("late_fees_amount", uil_df2.late_fees_amount.cast("decimal(10,2)"))\
    .withColumn("amount", uil_df2.amount.cast("decimal(10,2)"))

    uil_df2 = uil_df2.withColumnRenamed("amount", "total_amount")

    ai_final_cols = ['loan_id', 'loan_uuid','active_installment_id','active_installment_uuid', 'total_amount',
                      'interest_amount','principal_amount','late_fees_amount','adjusted_effective_date',
                      'original_effective_date','etl_ingestion_time','source_file_creation_time']

    active_installments_df = uil_df2.select(ai_final_cols)
    
    # Print or show the DataFrame
    #active_installments_df.limit(2).show(truncate=False)
    print("active_installments_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in active_installments_df.collect()]
    
    return serialized_data

def loan_financial_owners_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df3 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df3 = uil_df3.filter(uil_df3.dark == False)
    uil_df3 = uil_df3.filter(uil_df3.financial_owners != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df3 = uil_df3.withColumn(col, get_json_object(uil_df3.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df3.portfolios_created_at.desc())
    uil_df3 = uil_df3.withColumn("rownum", row_number().over(window))
    uil_df3 = uil_df3.filter(uil_df3.rownum == 1)

    uil_df3 = uil_df3.withColumn("loan_id", uil_df3.loan_id.cast("int"))

    uil_df3 = uil_df3.withColumn('temp_explode_financial_owners', conversion_udf(uil_df3.financial_owners))
    uil_df3 = uil_df3.select("*", explode(uil_df3.temp_explode_financial_owners).alias("explode_financial_owners"))

    list_of_attributes_financial_owners =['uuid','legal_entity','financial_owner','financial_owner_type','financial_owner_eff_date',
                                         'financial_owner_termination_date', 'financial_owner_termination_reason',
                                         'marketplace_status','marketplace_premium','marketplace_final_principal_ar',
                                         'marketplace_final_interest_ar']
    for col in list_of_attributes_financial_owners:
        uil_df3 = uil_df3.withColumn(col, get_json_object(uil_df3.explode_financial_owners, f"$.{col}"))


    uil_df3 = uil_df3\
        .withColumn("marketplace_premium", uil_df3.marketplace_premium.cast('decimal(10,4)'))\
        .withColumn("marketplace_final_principal_ar_amount", uil_df3.marketplace_final_principal_ar.cast('decimal(10,2)'))\
        .withColumn("marketplace_final_interest_ar_amount", uil_df3.marketplace_final_interest_ar.cast('decimal(10,2)'))\
        .withColumn("financial_owner_name", uil_df3.financial_owner)\
        .withColumn("effective_date", uil_df3.financial_owner_eff_date.cast("date"))\
        .withColumn("temp_termination_date", uil_df3.financial_owner_termination_date.cast("date"))\
        .withColumn("temp_termination_reason", uil_df3.financial_owner_termination_reason)\
        .withColumnRenamed("uuid","loan_financial_owner_uuid")  # CR 16348

    final_uil_df = uil_df3\
        .withColumn("termination_date", when(uil_df3.temp_termination_reason == "Debt Sale", None).otherwise(uil_df3.temp_termination_date))\
        .withColumn("termination_reason", when(uil_df3.temp_termination_reason == "Debt Sale", None).otherwise(uil_df3.temp_termination_reason))\
        .withColumn("debt_sale_termination_date", when(uil_df3.temp_termination_reason == "Debt Sale", uil_df3.temp_termination_date).otherwise(None))


    final_cols = ['loan_id','loan_uuid','loan_financial_owner_uuid','legal_entity','financial_owner_name','financial_owner_type',
                  'effective_date','termination_date','termination_reason','marketplace_status', 'debt_sale_termination_date',
                  'marketplace_premium','marketplace_final_principal_ar_amount','marketplace_final_interest_ar_amount',
                  'etl_ingestion_time', 'source_file_creation_time']

    loan_financial_owner_df = final_uil_df.select(final_cols)
    
    # Print or show the DataFrame
    #loan_financial_owner_df.limit(2).show(truncate=False)
    print("loan_financial_owners_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_financial_owner_df.collect()]
    
    return serialized_data

def operational_charge_off_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df4 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df4 = uil_df4.filter(uil_df4.dark == False)
    uil_df4 = uil_df4.filter(uil_df4.treasury_charge_offs != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df4 = uil_df4.withColumn(col, get_json_object(uil_df4.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df4.portfolios_created_at.desc())
    uil_df4 = uil_df4.withColumn("rownum", row_number().over(window))
    uil_df4 = uil_df4.filter(uil_df4.rownum == 1)

    uil_df4 = uil_df4.withColumn('temp_explode_operational_charge_offs', conversion_udf(uil_df4.operational_charge_offs))
    uil_df4 = uil_df4.select("*", explode(uil_df4.temp_explode_operational_charge_offs).alias("explode_operational_charge_offs"))

    list_of_attributes_operational_charge_offs = ['uuid','charge_off_date', 'charge_off_reason', 'forgiveness_date',
                                                  'write_off_amount', 'charge_off_amount']
    for col in list_of_attributes_operational_charge_offs:
        uil_df4 = uil_df4.withColumn(col, get_json_object(uil_df4.explode_operational_charge_offs, f"$.{col}"))


    uil_df4 = uil_df4.withColumn('charge_off_fees_amount', get_json_object(uil_df4.charge_off_amount, '$.fees'))
    uil_df4 = uil_df4.withColumn('charge_off_interest_amount', get_json_object(uil_df4.charge_off_amount, '$.interest'))
    uil_df4 = uil_df4.withColumn('charge_off_principal_amount', get_json_object(uil_df4.charge_off_amount, '$.principal'))

    uil_df4 = uil_df4\
        .withColumn("charge_off_date", uil_df4.charge_off_date.cast("date"))\
        .withColumn("forgiveness_date", uil_df4.forgiveness_date.cast("date"))\
        .withColumn("write_off_amount", uil_df4.write_off_amount.cast("double"))\
        .withColumn("charge_off_fees_amount", uil_df4.charge_off_fees_amount.cast("double"))\
        .withColumn("charge_off_interest_amount", uil_df4.charge_off_interest_amount.cast("double"))\
        .withColumn("charge_off_principal_amount", uil_df4.charge_off_principal_amount.cast("double"))\
        .withColumn("loan_id", uil_df4.loan_id.cast("long"))\
        .withColumn("servicing_account_id", uil_df4.servicing_account_id.cast("long"))\
        .withColumnRenamed("uuid","operational_charge_off_uuid")
        

    final_cols = ['loan_id', 'loan_uuid', 'operational_charge_off_uuid', 'servicing_account_id', 'charge_off_date', 'charge_off_reason',
                  'forgiveness_date', 'write_off_amount', 'charge_off_fees_amount', 'charge_off_interest_amount', 'charge_off_principal_amount',
                  'source_file_creation_time','etl_ingestion_time']

    operational_charge_offs_df = uil_df4.select(final_cols)
   
    
    # Print or show the DataFrame
    #operational_charge_offs_df.limit(2).show(truncate=False)
    print("operational_charge_off_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in operational_charge_offs_df.collect()]
    
    return serialized_data

def original_installment_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df5 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df5 = uil_df5.filter(uil_df5.dark == False)
    uil_df5 = uil_df5.filter(uil_df5.original_installments != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df5 = uil_df5.withColumn(col, get_json_object(uil_df5.loan_data, f"$.{col}"))

    window = Window.partitionBy('loan_id').orderBy(uil_df5.portfolios_created_at.desc())
    uil_df5 = uil_df5.withColumn("rownum", row_number().over(window))
    uil_df5 = uil_df5.filter(uil_df5.rownum == 1)

    uil_df5 = uil_df5.withColumn('temp_explode_original_installments', conversion_udf(uil_df5.original_installments))
    uil_df5 = uil_df5.select("*", explode(uil_df5.temp_explode_original_installments).alias("explode_original_installments"))

    list_of_attributes_original_installments = ['id','installment_date', 'original_interest', 'original_principal', 'installment_schedule_id']
    for col in list_of_attributes_original_installments:
        uil_df5 = uil_df5.withColumn(col, get_json_object(uil_df5.explode_original_installments, f"$.{col}"))

    uil_df5 = uil_df5\
        .withColumn("original_installment_uuid",  sha1(concat(uil_df5.id)))\
        .withColumn("loan_id", uil_df5.loan_id.cast("long"))\
        .withColumn("original_installment_id", uil_df5.id.cast("int"))\
        .withColumn("installment_date", uil_df5.installment_date.cast("date"))\
        .withColumn("interest_amount", uil_df5.original_interest.cast("decimal(10,2)"))\
        .withColumn("principal_amount", uil_df5.original_principal.cast("decimal(10,2)"))\
        .withColumn("installment_schedule_id", uil_df5.installment_schedule_id.cast("int"))

    window_p1 = Window.partitionBy('loan_id', 'installment_date').orderBy(uil_df5.original_installment_id)
    uil_df5 = uil_df5.withColumn("row_num", row_number().over(window_p1))
    uil_df5 = uil_df5.filter(uil_df5.row_num == 1)

    oi_final_cols = ['loan_id', 'loan_uuid','original_installment_id','installment_date',
                     'interest_amount','principal_amount','installment_schedule_id',
                     'original_installment_uuid','etl_ingestion_time','source_file_creation_time']

    original_installment_df = uil_df5.select(oi_final_cols)
    original_installment_df = original_installment_df.limit(100)
   
    
    # Print or show the DataFrame
    #original_installment_df.limit(2).show(truncate=False)
    print("original_installment_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in original_installment_df.collect()]
    
    return serialized_data

def treasury_charge_off_transformation():
    
    def conversion(my_data):
        if my_data == "[]" or my_data is None:
            return None
        data = json.loads(my_data)
        if isinstance(data, dict):
            return [my_data]
        elif isinstance(data, list):
            return [json.dumps(each) for each in data]
        else:
            return None

    # Read parquet file from S3
    uil_df6 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    conversion_udf = udf(conversion, ArrayType(StringType()))
    
    # Apply initial filters
    uil_df6 = uil_df6.filter((uil_df6.dark == False) & (uil_df6.treasury_charge_offs != '[]'))

    # Extract loan data attributes
    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col in list_of_attributes_loan_data:
        uil_df6 = uil_df6.withColumn(col, F.get_json_object(uil_df6.loan_data, f"$.{col}"))

    # Apply window function
    window = Window.partitionBy('loan_id').orderBy(uil_df6.portfolios_created_at.desc())
    uil_df6 = uil_df6.withColumn("rownum", F.row_number().over(window))
    uil_df6 = uil_df6.filter(uil_df6.rownum == 1)

    # Apply the rest of the transformations
    uil_df6 = uil_df6.withColumn('temp_explode_treasury_charge_offs', conversion_udf(uil_df6.treasury_charge_offs))
    uil_df6 = uil_df6.select("*", F.explode(uil_df6.temp_explode_treasury_charge_offs).alias("explode_treasury_charge_offs"))

    list_of_attributes_treasury_charge_offs = ['uuid', 'effective_date', 'charge_off_reason', 'id', 'transaction_timestamp',
                                               'servicing_account_id','reverted_at','created_at','updated_at',
                                               'source_uuid','source_type','revertible']
    for col in list_of_attributes_treasury_charge_offs:
        uil_df6 = uil_df6.withColumn(col, F.get_json_object(uil_df6.explode_treasury_charge_offs, f"$.{col}"))

    uil_df6 = uil_df6\
        .withColumn("created_time", F.from_utc_timestamp(uil_df6.transaction_timestamp, 'CST'))\
        .withColumn("reverted_time", F.from_utc_timestamp(uil_df6.reverted_at, 'CST'))\
        .withColumn("updated_time", F.from_utc_timestamp(uil_df6.updated_at, 'CST'))\
        .withColumn("effective_date", uil_df6.effective_date.cast("date"))\
        .withColumn("treasury_charge_off_id", uil_df6.id.cast("int"))\
        .withColumn("servicing_account_id", uil_df6.servicing_account_id.cast("long"))\
        .withColumn("revertible_flag", uil_df6.revertible.cast("boolean"))\
        .withColumn("loan_id", uil_df6.loan_id.cast("long"))\
        .withColumnRenamed("uuid","treasury_charge_off_uuid") #CR 16348

    uil_df6 = uil_df6.filter(uil_df6.treasury_charge_off_uuid.isNotNull())

    tco_select_cols = ['loan_id','loan_uuid','treasury_charge_off_uuid','effective_date','charge_off_reason',
                       'treasury_charge_off_id','servicing_account_id','reverted_time',
                       'created_time','updated_time','source_uuid','source_type','revertible_flag',
                       'etl_ingestion_time','source_file_creation_time']
    treasury_charge_off_df = uil_df6.select(tco_select_cols)
    
    # Print or show the DataFrame
    #treasury_charge_off_df.limit(2).show(truncate=False)
    print("treasury_charge_off_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in treasury_charge_off_df.collect()]
    
    return serialized_data

def loan_payment_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df7 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df7 = uil_df7.filter(uil_df7.dark == False)
    uil_df7 = uil_df7.filter(uil_df7.payment_plans != '[]')

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid', 'payment_method','origination_state']
    for col in list_of_attributes_loan_data:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.loan_data, f"$.{col}"))


    window = Window.partitionBy('loan_id').orderBy(uil_df7.portfolios_created_at.desc())
    uil_df7 = uil_df7.withColumn("rownum", row_number().over(window))
    uil_df7 = uil_df7.filter(uil_df7.rownum == 1)

    list_of_attributes_customer_information = ['customer_id']
    for col in list_of_attributes_customer_information:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.customer_information, f"$.{col}"))


    uil_df7 = uil_df7.withColumn('temp_explode_payment_plans', conversion_udf(uil_df7.payment_plans))
    uil_df7 = uil_df7.select("*", explode(uil_df7.temp_explode_payment_plans).alias("explode_payment_plans"))

    list_of_attributes_payment_plans = ['status','lock_in_date','total_amount','payment_amount','adjusted_end_date',
                                        'first_payment_date','early_terminate_date','adjusted_lock_in_date','tdr',
                                        'id','start_date','end_date','created_timestamp','plan_type','uuid','frequency',
                                        'contract_id','contract_signed_datetime','created_by_id','payment_dates','rule_version',
                                        'freezes_lateness', 'lateness_reset_date', 'updated_at']

    for col in list_of_attributes_payment_plans:
        uil_df7 = uil_df7.withColumn(col, get_json_object(uil_df7.explode_payment_plans, f"$.{col}"))



    uil_df7 = uil_df7\
        .withColumn("structure", uil_df7.plan_type)\
        .withColumn("created_time", from_utc_timestamp(uil_df7.created_timestamp, 'CST'))

    uil_df7 = uil_df7 \
        .withColumn("grace_period_days", when(uil_df7.origination_state.isin('CA', 'MO'), lit(15)).otherwise(lit(10)).cast("int")) \
        .withColumn("loan_uuid", uil_df7.loan_uuid)\
        .withColumn("payment_method", uil_df7.payment_method)\
        .withColumn("customer_id", uil_df7.customer_id.cast("int"))\
        .withColumn("loan_id", uil_df7.loan_id.cast("int"))\
        .withColumn("lock_in_date", uil_df7.lock_in_date.cast("date"))\
        .withColumn("loan_payment_plan_id", uil_df7.id.cast("int"))\
        .withColumn("contract_id", uil_df7.contract_id.cast("int"))\
        .withColumn("created_by_portal_id", uil_df7.created_by_id.cast("int"))\
        .withColumn("contract_signed_date", uil_df7.contract_signed_datetime.cast("date"))\
        .withColumn("rule_version", uil_df7.rule_version)\
        .withColumn("total_amount", uil_df7.total_amount.cast("decimal(12,2)"))\
        .withColumn("payment_amount", uil_df7.payment_amount.cast("decimal(12,2)"))\
        .withColumn("completed_date",
                    when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"),
                         coalesce(uil_df7.adjusted_end_date.cast("date"), uil_df7.end_date.cast("date")))\
                    .otherwise(None).cast("date"))\
        .withColumn("adjusted_lock_in_date",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"),
                         coalesce(uil_df7.adjusted_end_date, uil_df7.end_date))\
                    .otherwise(uil_df7.adjusted_lock_in_date).cast("date"))\
        .withColumn("adjusted_end_date",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "terminated"), uil_df7.early_terminate_date)\
                    .when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')),
                          coalesce(uil_df7.adjusted_end_date, uil_df7.early_terminate_date, uil_df7.end_date))\
                    .otherwise(None).cast("date"))\
        .withColumn("first_payment_date", uil_df7.first_payment_date.cast("date"))\
        .withColumn("early_terminate_date", uil_df7.early_terminate_date.cast("date"))\
        .withColumn("start_date",
                    when(uil_df7.structure == 'deferment', uil_df7.created_time.cast("date"))\
                    .when(uil_df7.structure == 'long_term', uil_df7.created_time.cast("date"))\
                    .otherwise(uil_df7.start_date).cast("date"))\
        .withColumn("end_date",
                    when(~(uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')), uil_df7.end_date)\
                    .otherwise(None).cast("date"))\
        .withColumn("status",
                    when((uil_df7.structure.isin('loan_mod_trial', 'term_extension_trial')) & (uil_df7.status == "completed"), 'locked_in')\
                    .otherwise(uil_df7.status))\
        .withColumn("structure",
                    when(uil_df7.structure == 'loan_mod_trial', 'apr_reduction')\
                    .when(uil_df7.structure == 'term_extension_trial','term_extension')\
                    .otherwise(uil_df7.structure))\
        .withColumn("loan_payment_plan_uuid", uil_df7.uuid)\
        .withColumn("payment_dates_text", uil_df7.payment_dates)\
        .withColumn("frequency", uil_df7.frequency)\
        .withColumn("troubled_debt_restructuring_flag", uil_df7.tdr.cast("boolean"))\
        .withColumn("freezes_lateness_flag", uil_df7.freezes_lateness.cast("boolean"))\
        .withColumn("lateness_reset_date", uil_df7.lateness_reset_date.cast("date"))\
        .withColumn("updated_time", from_utc_timestamp(uil_df7.updated_at, 'CST'))


    lpp_select_cols = ['loan_id','loan_uuid','payment_method','customer_id','grace_period_days','status',
                       'lock_in_date','total_amount','payment_amount','adjusted_end_date','first_payment_date',
                       'early_terminate_date','adjusted_lock_in_date','troubled_debt_restructuring_flag',
                       'loan_payment_plan_id','start_date','end_date','created_time','structure','loan_payment_plan_uuid',
                       'frequency','contract_id','contract_signed_date','created_by_portal_id','payment_dates_text',
                       'rule_version','completed_date', 'lateness_reset_date', 'freezes_lateness_flag',
                       'etl_ingestion_time', 'source_file_creation_time', 'updated_time']

    loan_payment_plans_df = uil_df7.select(lpp_select_cols)
    
    # Print or show the DataFrame
    #loan_payment_plans_df.limit(2).show(truncate=False)
    print("loan_payment_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_payment_plans_df.collect()]
    
    return serialized_data

def interest_rate_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None

    uil_df8 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))

    uil_df8 = uil_df8.filter(uil_df8.dark == False)
    
 
    uil_df8 = uil_df8.withColumn('interest_rate_schedule', get_json_object(col('loan_data'),'$.interest_rate_schedule'))

    list_of_attributes_loan_data = ['loan_id', 'loan_uuid']
    for col_name in list_of_attributes_loan_data:
        uil_df8 = uil_df8.withColumn(col_name, get_json_object(uil_df8.loan_data, f"$.{col}"))



    window = Window.partitionBy('loan_id').orderBy(uil_df8.portfolios_created_at.desc())
    uil_df8 = uil_df8.withColumn("rownum", row_number().over(window))
    uil_df8 = uil_df8.filter(uil_df8.rownum == 1)

    uil_df8 = uil_df8.withColumn('temp_explode_interest_rate_schedule',conversion_udf(uil_df8.interest_rate_schedule))
    uil_df8 = uil_df8.select("*", explode(uil_df8.temp_explode_interest_rate_schedule).alias("explode_interest_rate_schedule"))

    list_of_attributes_interest_rate_schedule = ['interest_rate', 'effective_date', 'id', 'inactive', 'created_at',
                                                     'updated_at', 'source_type', 'source_uuid', 'end_date', 'cap']
    for col_name in list_of_attributes_interest_rate_schedule:
        uil_df8 = uil_df8.withColumn(col_name, get_json_object(uil_df8.explode_interest_rate_schedule, f"$.{col}"))


    uil_df8 = uil_df8.withColumn('effective_date',when(uil_df8['effective_date'] < '1900-01-01',to_date(substring(uil_df8['effective_date'],3,10),'MM-dd-yy')).otherwise(uil_df8['effective_date']))

    uil_df8 = uil_df8\
        .withColumn("interest_rate_uuid",  sha1(concat(uil_df8.loan_id, uil_df8.interest_rate, uil_df8.effective_date)))\
        .withColumn("loan_id", uil_df8.loan_id.cast("long"))\
        .withColumn("interest_rate", uil_df8.interest_rate.cast("float"))\
        .withColumn("effective_date", uil_df8.effective_date.cast("date"))\
        .withColumn("interest_rate_id", uil_df8.id.cast("long"))\
        .withColumn("created_time", from_utc_timestamp(uil_df8.created_at, 'CST'))\
        .withColumn("updated_time", from_utc_timestamp(uil_df8.updated_at, 'CST'))\
        .withColumn("source_uuid", uil_df8.source_uuid)\
        .withColumn("source_type", uil_df8.source_type)\
        .withColumn("end_date", uil_df8.end_date.cast("date"))\
        .withColumn("cap_flag", uil_df8.cap.cast("boolean"))\
        .withColumn("inactive_flag", uil_df8.inactive.cast("boolean"))

    uil_df8 = uil_df8.filter(uil_df8.interest_rate_uuid.isNotNull())

    irs_select_cols = ['loan_id','loan_uuid','interest_rate','effective_date','interest_rate_uuid',
                           'interest_rate_id','inactive_flag','created_time','updated_time','source_type',
                           'source_uuid','end_date','cap_flag','etl_ingestion_time','source_file_creation_time']

    interest_rate_schedule_df = uil_df8.select(irs_select_cols)
    
    # Print or show the DataFrame
    #interest_rate_schedule_df.limit(2).show(truncate=False)
    print("interest_rate_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in interest_rate_schedule_df.collect()]
    
    return serialized_data

def bankruptacy_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None
    # Define schema for the JSON data
    json_schema = StructType().add("claimed_date", StringType())

    uil_df9 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df9 = uil_df9.filter(uil_df9.dark == False)
    # Filter to only keep the most recent record per loan_id
    window = Window.partitionBy('loan_id').orderBy(uil_df9.portfolios_created_at.desc())
    uil_df9 = uil_df9.withColumn("rownum", row_number().over(window))
    uil_df9 = uil_df9.filter(uil_df9.rownum == 1)

    # Filter to remove any row where bankruptcy_timeline = '[]' and bankruptcy_claims = '[]'
    uil_df9 = uil_df9.filter((uil_df9.bankruptcy_timeline != "[]") & (uil_df9.bankruptcy_claims != "[]"))
    

    # Starting with the output of step 4, filter to rows where bankruptcy_timeline <> '[]' then explode bankruptcy_timeline column to one row per bankruptcy per loan
    uil_df9 = uil_df9.filter(uil_df9.bankruptcy_timeline != "[]")
    uil_df9 = uil_df9.withColumn("temp_bk_num", row_number().over(Window.partitionBy("loan_id").orderBy("portfolios_created_at")))
    uil_df9 = uil_df9.withColumn("temp_bk_id", sha1(concat("loan_id", lit("timeline"), "temp_bk_num")))

    # Starting with the output of step 4, filter to rows where bankruptcy_claims <> '[]' then explode bankruptcy_claims column to one row per bankruptcy per loan
    uil_df9 = uil_df9.filter(uil_df9.bankruptcy_claims != "[]")
    uil_df9 = uil_df9.withColumn("claimed_date", from_json("bankruptcy_claims", json_schema).getItem("claimed_date"))
    uil_df9 = uil_df9.withColumn("temp_claim_id", sha1(concat("loan_id", lit("claimed"), "claimed_date")))

    # Drop temporary fields
    uil_df9 = uil_df9.drop("temp_bk_num", "temp_claim_id")



    required_cols = ['loan_id', 'portfolios_contract_name', 'portfolios_created_at', 'dark', 'installment_performance',
                     'financial_transactions', 'validation_of_debt_notices', 'customer_payment_authorizations',
                     'nsf_fees', 'settlements', 'payment_plans', 'delinquency_amounts_history', 'accounts_status',
                     'accounts_contract_name', 'accounts_created_at', 'accounts_updated_at', 'process_date',
                     'etl_ingestion_time', 'source_file_creation_time']

    bankrupty_df = uil_df9.select(required_cols)
    #bankrupty_df.limit(2).show(truncate = False)
    # Convert DataFrame to list of dictionaries
    print("bankruptacy_transformation successful")
    serialized_data = [row.asDict() for row in bankrupty_df.collect()]
    
    return serialized_data

def loan_collections_transformation():
    
    def conversion(my_data):
            if my_data == "[]" or my_data is None:
                return None
            data = json.loads(my_data)
            if isinstance(data, dict):
                return [my_data]
            elif isinstance(data, list):
                return [json.dumps(each) for each in data]
            else:
                return None
    # Define schema for the JSON data
    json_schema = StructType().add("claimed_date", StringType())
    uil_df10 = spark.read.parquet(f"s3a://{bucket_name}/{key}", header=True, inferSchema=True)
    
    conversion_udf = udf(conversion, ArrayType(StringType()))
    uil_df10 = uil_df10.filter(uil_df10.dark == False)

    # Filter to only keep the most recent record per loan_id
    window = Window.partitionBy('loan_id').orderBy(uil_df10.portfolios_created_at.desc())
    uil_df10 = uil_df10.withColumn("rownum", row_number().over(window))
    uil_df10 = uil_df10.filter(uil_df10.rownum == 1)

    # Filter to remove any row where collection_cases = '[]'
    uil_df10 = uil_df10.filter(uil_df10.collection_cases != '[]')

    # Explode collection_cases column to one row per owner per loan
    uil_df10 = uil_df10.withColumn("collection_case", explode(from_json(col("collection_cases"), ArrayType(StructType([
        StructField("case_type", StringType(), True),
        StructField("start_date", StringType(), True),  # Assuming start_date is StringType, change if different
        StructField("end_date", StringType(), True),    # Assuming end_date is StringType, change if different
        StructField("agency_name", StringType(), True),
        StructField("agency_email", StringType(), True),
        StructField("agency_phone", StringType(), True),
        StructField("poa_effective_date", StringType(), True)  # Assuming poa_effective_date is StringType, change if different
    ])))))

    # Hash loan_id and start_date to create loan_collection_case_uuid
    uil_df10 = uil_df10.withColumn("loan_collection_case_uuid", sha1(concat_ws("", uil_df10["loan_id"], uil_df10["collection_case.start_date"])))

    # Select required columns
    required_cols = ['loan_id', 'portfolios_contract_name', 'portfolios_created_at', 'dark', 'installment_performance',
                     'financial_transactions', 'validation_of_debt_notices', 'customer_payment_authorizations',
                     'nsf_fees', 'settlements', 'payment_plans', 'delinquency_amounts_history', 'accounts_status',
                     'accounts_contract_name', 'accounts_created_at', 'accounts_updated_at', 'process_date',
                     'etl_ingestion_time', 'source_file_creation_time', 'loan_collection_case_uuid']

    loan_collection_df = uil_df10.select(required_cols)
    #loan_collection_df.limit(2).show()
    print("loan_collections_transformation successful")
    # Convert DataFrame to list of dictionaries
    serialized_data = [row.asDict() for row in loan_collection_df.collect()]
    
    return serialized_data





start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

watch_file_drop_task = ShortCircuitOperator(
    task_id='watch_file_drop',
    python_callable=watch_file_drop,
    provide_context=True,
    dag=dag,
)

validation_schema_task = PythonOperator(
    task_id='validation_schema',
    python_callable=validation_schema,
    dag=dag,
)

loan_debt_sale_transformation_task = PythonOperator(
    task_id='loan_debt_sale_transformation',
    python_callable=loan_debt_sale_transformation,
    provide_context=True,
    dag=dag,
)

loan_servicing_settlements_transformation_task = PythonOperator(
    task_id='loan_servicing_settlements_transformation',
    python_callable=loan_servicing_settlements_transformation,
    provide_context=True,
    dag=dag,
)

active_installments_transformation_task = PythonOperator(
    task_id='active_installments_transformation',
    python_callable=active_installments_transformation,
    provide_context=True,
    dag=dag,
)

loan_financial_owners_transformation_task = PythonOperator(
    task_id='loan_financial_owners_transformation',
    python_callable=loan_financial_owners_transformation,
    provide_context=True,
    dag=dag,
)

operational_charge_off_transformation_task = PythonOperator(
    task_id='operational_charge_off_transformation',
    python_callable=operational_charge_off_transformation,
    provide_context=True,
    dag=dag,
)

original_installment_transformation_task = PythonOperator(
    task_id='original_installment_transformation',
    python_callable=original_installment_transformation,
    provide_context=True,
    dag=dag,
)
treasury_charge_off_transformation_task = PythonOperator(
    task_id='treasury_charge_off_transformation',
    python_callable=treasury_charge_off_transformation,
    provide_context=True,
    dag=dag,
)

loan_payment_transformation_task = PythonOperator(
    task_id='loan_payment_transformation',
    python_callable=loan_payment_transformation,
    provide_context=True,
    dag=dag,
)

interest_rate_transformation_task = PythonOperator(
    task_id='interest_rate_transformation',
    python_callable=interest_rate_transformation,
    provide_context=True,
    dag=dag,
)
bankruptacy_transformation_task = PythonOperator(
    task_id='bankruptacy_transformation',
    python_callable=bankruptacy_transformation,
    provide_context=True,
    dag=dag,
)
loan_collections_transformation_task = PythonOperator(
    task_id='loan_collections_transformation',
    python_callable=loan_collections_transformation,
    provide_context=True,
    dag=dag,
)

start >> watch_file_drop_task >> validation_schema_task >> [loan_debt_sale_transformation_task, loan_servicing_settlements_transformation_task,active_installments_transformation_task,loan_financial_owners_transformation_task, operational_charge_off_transformation_task,original_installment_transformation_task,treasury_charge_off_transformation_task,loan_payment_transformation_task,interest_rate_transformation_task,bankruptacy_transformation_task,loan_collections_transformation_task] >> end
