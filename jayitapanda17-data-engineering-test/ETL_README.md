<h3>Approach:</h3>
<br><b>Ingestion :</b>
As per requirement, I would use Spark to process the data.
At first, we will read the data from INPUT directory and create dataframe.
While reading the file and creating dataframe I am  also defining schema <br>

<br><b>Transformation :</b>
After creating spark dataframe I have filtered the menus containing beef,
used lower function as beef could be either 'Beef' or 'beef'.
Post that I have extracted the information for cookTime nd prepTime in minutes,
it helped me later to do  the actual calculation.I have used regular expression to
calculate the times from ISO format.
Calculated total_cook_time adding cookTime and prepTime
Calculated difficulty levels based on the condition given
easy - less than 30 mins
medium - between 30 and 60 mins
hard - more than 60 mins.
Finally calculated average cooking time duration per difficulty level used 
a groupBy on the final dataframe.</br>

<br><b>WritingOutput: </b>
After all the transformations saved the dataframe as a csv file used coalesce(1) so that whole data 
saved to one file in OUTPUT folder.</br>

<h3>Spark-Submit:</h3>
<br>For local run please change the path if your code path is different .I have provided the path which I have used 
spark-submit --master local /Users/jpanda/Desktop/Books/Code/jayitapanda17-data-engineering-test/code/task.py</br>

<br>For Cluster run we have to change the master to YARN also we can set driver,number of executor cores and
memory according to the data size and volume.For cluster deployment we can zip the whole code and run as a package
for example spark-submit --master yarn --driver-memory 4g --num-executors 8 --executor-memory 4g 
--conf spark.executor.memoryOverhead=2048 --conf spark.executor.cores=5 --conf spark.driver.maxResultSize=0 
--conf spark.yarn.maxAppAttempts=4</br>


<h3>Config Management :</h3>
<br>I have created a config file /config/task.cfg
where created few variables ,if someone wants to change inputfile path,output path ,source  file type ,target file type
it can be managed from here.Referenced the same file in main() in the main code task.py</br>

<h3>Logging :</h3>
<br>Created a new python class  logging_helper.py for logging.All the logs will be stored for each run
and unique files will be created with the format load_process_timestamp.log in the logs folder.</br>


<h3>Data Quality:</h3>
<br>Created a separate python program dq_check.py to verify below
validate_file_type,validate_num_source_column_count,validate_target_row_count,validate_target_col_count.
The similar details are given in config file.We could have checked the source count as well
but its not a good practice to run count if the source data is huge in spark.</br>

<h3>Unit Test:</h3>

<br>ried to write unit test cases using pytest module and unittest module but somehow ther are some issues coming 
with the test files not able to import pyspark.These are inside tests folder.</br>


<h3>CI/CD:</h3>

<br>We can create Jenkins pipeline for continuous integration and deployment process,
Before that we need to create release versions for that code repositary
and deploy only the version.If we are using could native solution
we can deploy in EC2 as it will help us with a computing Capacity,But
we also need help of EMR/AWS Glue to run the Spark jobs as this application is 
created using pyspark.</br>

<h3>Diagnosis of performance Issue :</h3>

<br>We can monitor the jobs from Spark UI and if for some reason we see
stages are stuck or failing we can optimise the jobs.We can use repartition in case of skewness also we can control
suffle partitions in case of very huge file.We can also optimise the spark joins in case of any performance issue.</br>

<h3>Schedule :</h3>

<br>We can use Apache Airflow as a schedular here ,can create a DAG
where we can mention like below code

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
dag = DAG(
    'dag_name',
    default_args=default_args,
    description='How to use the Python Operator?',
    schedule_interval=timedelta(days=1),
)

spark_submit_cmd = 'source env_export.sh ; $SPARK_SUBMIT  --master spark://$MASTER_PRIVATE_IP:7077 --total-executor-cores 10 --executor-memory 1G  $SCRIPTS_DIR/task.py

t1 = PythonOperator(
    task_id='print',
    bash_command= spark_submit_cmd,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t1</br>



