from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, to_date, lit, udf,to_timestamp
from pyspark.sql.types import StringType, BooleanType,IntegerType
import re
from pyspark.sql.functions import from_utc_timestamp, date_format


# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()






def validate_business_logic(
    referral_status, reward_value, transaction_id, 
    transaction_status, transaction_type, transaction_at, 
    referral_at, is_deleted, referrer_membership_expired, is_reward_granted):
    
    # Check for Valid Referral Rewards - Condition 1
    if (referral_status == 'Berhasil' and 
        reward_value is not None and reward_value > 0 and 
        transaction_id is not None and 
        transaction_status == 'PAID' and 
        transaction_type == 'NEW' and 
        transaction_at is not None and referral_at is not None and 
        transaction_at > referral_at and 
        transaction_at.month == referral_at.month and 
        not is_deleted and 
        not referrer_membership_expired and 
        is_reward_granted):
        return True
    
    # Check for Valid Referral Rewards - Condition 2
    if (referral_status in ['Menunggu', 'Tidak Berhasil'] and 
        (reward_value is None or reward_value == 0)):
        return True

    # Check for Invalid Referral Rewards - Condition 1
    if (reward_value is not None and reward_value > 0 and 
        referral_status != 'Berhasil'):
        return False
    
    # Check for Invalid Referral Rewards - Condition 2
    if (reward_value is not None and reward_value > 0 and 
        transaction_id is None):
        return False
    
    # Check for Invalid Referral Rewards - Condition 3
    if ((reward_value is None or reward_value == 0) and 
        transaction_id is not None and 
        transaction_status == 'PAID' and 
        transaction_at is not None and referral_at is not None and
        transaction_at > referral_at):
        return False
    
    # Check for Invalid Referral Rewards - Condition 4
    if (referral_status == 'Berhasil' and 
        (reward_value is None or reward_value == 0)):
        return False
    
    # Check for Invalid Referral Rewards - Condition 5
    if (transaction_at is not None and referral_at is not None and
        transaction_at < referral_at):
        return False
    
    # If none of the conditions are met, return False by default
    return False





# Read CSV files
user_referrals_df = spark.read.csv('/home/jovyan/data/user_referrals.csv', header=True, inferSchema=True)
user_referrals_status_df = spark.read.csv('/home/jovyan/data/user_referral_statuses.csv', header=True, inferSchema=True)
user_referral_logs_df = spark.read.csv('/home/jovyan/data/user_referral_logs.csv', header=True, inferSchema=True)
referral_rewards_df = spark.read.csv('/home/jovyan/data/referral_rewards.csv', header=True, inferSchema=True)
paid_transactions_df = spark.read.csv('/home/jovyan/data/paid_transactions.csv', header=True, inferSchema=True)
lead_log_df = spark.read.csv('/home/jovyan/data/lead_log.csv', header=True, inferSchema=True)
user_logs_df = spark.read.csv('/home/jovyan/data/user_logs.csv', header=True, inferSchema=True)



# Drop duplicates and sort data cleaning
lead_log_df = lead_log_df.orderBy(['created_at', 'id']).dropDuplicates(['lead_id', 'created_at'])
user_logs_df = user_logs_df.orderBy(['user_id', 'membership_expired_date']).dropDuplicates(['user_id'])
user_referral_logs_df = user_referral_logs_df.orderBy(['user_referral_id', 'created_at']).dropDuplicates(['user_referral_id'])



user_referral_logs_df = user_referral_logs_df.withColumn('created_at', date_format(from_utc_timestamp('created_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))
user_referral_logs_df = user_referral_logs_df.withColumnRenamed('created_at', 'user_referral_logs_created_at')
user_referral_logs_df = user_referral_logs_df.withColumnRenamed('id', 'user_referral_logs_id')


user_logs_df = user_logs_df.withColumn('membership_expired_date', date_format(from_utc_timestamp('membership_expired_date', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))
lead_log_df = lead_log_df.withColumn('created_at', date_format(from_utc_timestamp('created_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))
user_referrals_df = user_referrals_df.withColumn('updated_at', date_format(from_utc_timestamp('updated_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))

paid_transactions_df = paid_transactions_df.withColumn('transaction_at', date_format(from_utc_timestamp('transaction_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))



referral_rewards_df = referral_rewards_df.withColumn('created_at', date_format(from_utc_timestamp('created_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))
referral_rewards_df = referral_rewards_df.withColumnRenamed('created_at', 'reward_granted_at')


user_referrals_status_df = user_referrals_status_df.withColumn('created_at', date_format(from_utc_timestamp('created_at', 'Asia/Jakarta'), 'yyyy-MM-dd HH:mm:ss'))
user_referrals_status_df = user_referrals_status_df.withColumnRenamed('created_at', 'referral_statuses_created_at')



# Perform the join
fact_table = user_referrals_df \
    .join(user_referral_logs_df, user_referrals_df.referral_id == user_referral_logs_df.user_referral_id, 'left') \
    .join(lead_log_df, user_referrals_df.referee_id == lead_log_df.lead_id, 'left') \
    .join(referral_rewards_df, user_referrals_df.referral_reward_id == referral_rewards_df.id, 'left') \
    .join(user_logs_df, user_referrals_df.referrer_id == user_logs_df.user_id, 'left') \
    .join(paid_transactions_df, user_referrals_df.transaction_id == paid_transactions_df.transaction_id, 'left') \
    .join(user_referrals_status_df, user_referrals_df.user_referral_status_id == user_referrals_status_df.id, 'left') \
    .withColumnRenamed('description', 'referral_status')

# Drop the duplicate key columns from the right DataFrame
fact_table = fact_table.drop(
    user_referral_logs_df.user_referral_id, 
    lead_log_df.lead_id, 
    referral_rewards_df.id, 
    user_logs_df.user_id, 
    paid_transactions_df.transaction_id, 
    user_referrals_status_df.id
)


# Determine referral_source_category
fact_table = fact_table.withColumn('referral_source_category', when(fact_table['referral_source'] == 'User Sign Up', lit('Online'))
                                                     .when(fact_table['referral_source'] == 'Draft Transaction', lit('Offline'))
                                                     .otherwise(fact_table['source_category']))

# Convert reward_value to numeric
reward_value_udf = udf(lambda x: int(''.join(re.findall(r'\d+', str(x)))) if re.findall(r'\d+', str(x)) else None, IntegerType())
fact_table = fact_table.withColumn('reward_value', reward_value_udf(col('reward_value')))

fact_table = fact_table.withColumn('transaction_at', to_timestamp(col('transaction_at'), 'yyyy-MM-dd HH:mm:ss'))
fact_table = fact_table.withColumn('referral_at', to_timestamp(col('referral_at'), 'yyyy-MM-dd HH:mm:ss'))




validate_business_logic_udf = udf(validate_business_logic, BooleanType())

# Apply the UDF to the DataFrame
fact_table = fact_table.withColumn(
    'is_business_logic_valid', 
    validate_business_logic_udf(
        col('referral_status'),
        col('reward_value'),
        col('transaction_id'),
        col('transaction_status'),
        col('transaction_type'),
        col('transaction_at'),
        col('referral_at'),
        col('is_deleted'),
        col('membership_expired_date'),
        col('is_reward_granted')
    )
)


# select the column based on the instruction
final_fact_table = fact_table.select(
    "referral_id",
    "referral_source",
    "referral_at",
    "referrer_id",
    "name",
    "phone_number",
    "homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "reward_value",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_granted_at",
    "is_business_logic_valid",
    "user_referral_logs_id",
    "referral_source_category"
)


final_fact_table = final_fact_table.withColumnsRenamed({
    "user_referral_logs_id": "referral_details_id",
    "reward_value": "num_reward_days",
    "homeclub": "referrer_homeclub",
    "phone_number": "referrer_phone_number",
    "name": "referrer_name",
    
})

# Show the selected columns (optional, for verification)
#selected_columns_df.show()

final_fact_table.write.csv('/home/jovyan/data/output', header=True, mode='overwrite')


