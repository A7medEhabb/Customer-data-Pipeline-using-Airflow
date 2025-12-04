import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from faker import Faker
import pandas as pd
import random
from uuid import uuid4
import psycopg2
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
from airflow.hooks.base import BaseHook
import base64



def create_tables():
    conn=psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor=conn.cursor()


    cursor.execute("DROP TABLE customers CASCADE;")
    cursor.execute("DROP TABLE products CASCADE;")
    cursor.execute("DROP TABLE stores CASCADE;")
    cursor.execute("DROP TABLE transactions CASCADE;")
    


    #create customers table
    #UUID -> universal unique identifier 128 bit value like 550e8400-e29b-41d4-a716-446655440000 

    cursor.execute(
        """
        create table if not exists customers(
            customer_id UUID primary key, 
            first_name varchar(50),
            last_name varchar(50),
            email varchar(50),
            address text,
            city varchar(50),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(20),
            date_of_birth DATE,
            account_balance NUMERIC(10, 2),
            created_at TIMESTAMP
        );
        """
    )

    #create products table
    cursor.execute(
        """
        create table if not exists products(
            product_id serial primary key,
            product_name varchar(100),
            category varchar(100),
            price numeric(10,2),
            quantity int
        );
        """
    )
    #create stores table
    cursor.execute(
        """
        create table if not exists stores(
            store_id serial primary key,
            store_name varchar(50),
            location varchar(50),
            store_type varchar(50)
        );
        """
    )

    #create transactions table
    cursor.execute(
        """
        create table if not exists transactions(
            transaction_id UUID primary key,
            customer_id UUID references customers(customer_id),
            transaction_date timestamp,
            transaction_amount numeric(10,2),
            store_id int references stores(store_id),
            product_id int references products(product_id),
            quantity int,
            payment_method varchar(50)
        
        );
        """
    )
    conn.commit()
    cursor.close()
    conn.close()

faker_inst=Faker()

def generate_customer_data(num_customers=100):
    customers_data=[]
    for i in range(num_customers):
        customer={
            'customer_id': str(uuid4()),
            'first_name': faker_inst.first_name(),
            'last_name': faker_inst.last_name(),
            'email' : faker_inst.email(),
            'address' :faker_inst.address(),
            'city':faker_inst.city(),
            'state':faker_inst.state(),
            'country':faker_inst.country(),
            'postal_code': faker_inst.postcode(),
            'date_of_birth': faker_inst.date_of_birth(),
            'account_balance': round(random.uniform(0,1000000),2),
            'created_at': faker_inst.date_time_this_year()
        }
        customers_data.append(customer)
    df_customers=pd.DataFrame(customers_data)
    output_dir = '/opt/airflow/generated_data'
    os.makedirs(output_dir, exist_ok=True)

    df_customers.to_csv(os.path.join(output_dir, 'customers_data.csv'), index=False)


def generate_product_data(num_products=50):
    product_data=[]
    for i in range(num_products):
        product={
            'product_id': i+1,
            'product_name':faker_inst.word(),
            'category': random.choice(['Electronics','Clothing', 'Food', 'Books']),
            'price': round(random.uniform(10,1000),2),
            'quantity': random.randint(1,200)
        }
        product_data.append(product)
    df_products=pd.DataFrame(product_data)
    output_dir = '/opt/airflow/generated_data'
    os.makedirs(output_dir, exist_ok=True)
    df_products.to_csv(os.path.join(output_dir,'products_data.csv'), index=False)



def generate_stores_data(num_stores=20):
    store_data=[]
    for i in range(num_stores):
        store={
            'store_id': i+1,
            'store_name': faker_inst.company(),
            'location':faker_inst.country(),
            'store_type': random.choice(['Retail', 'Online'])
        }
        store_data.append(store)
    df_stores=pd.DataFrame(store_data)    
    output_dir = '/opt/airflow/generated_data'
    os.makedirs(output_dir, exist_ok=True)
    df_stores.to_csv(os.path.join(output_dir,'stores_data.csv'), index=False)

def generate_transaction_data(num_trans=1000):
    df_customers = pd.read_csv('/opt/airflow/generated_data/customers_data.csv')
    df_stores = pd.read_csv('/opt/airflow/generated_data/stores_data.csv')
    df_products = pd.read_csv('/opt/airflow/generated_data/products_data.csv')

    transactions=[]
    for i in range(num_trans):
        trans={
            'transaction_id':str(uuid4()),
            'customer_id': random.choice(df_customers['customer_id']),
            'transaction_date': faker_inst.date_time_this_year(),
            'transaction_amount': round(random.uniform(20,1000),2),
            'store_id': random.choice(df_stores['store_id']),
            'product_id': random.choice(df_products['product_id']),
            'quantity':random.randint(1,50),
            'payment_method': random.choice(['Cash', 'Credit Card', 'PayPal'])
        }
        transactions.append(trans)
    df_transactions=pd.DataFrame(transactions)
    output_dir = '/opt/airflow/generated_data'
    os.makedirs(output_dir, exist_ok=True)
    df_transactions.to_csv(os.path.join(output_dir,'transaction_data.csv'), index=False)



def calculate_metrics(**kwargs):
    folder='/opt/airflow/generated_data/'
    transaction_file=os.path.join(folder,'transaction_data.csv')
    df_transaction=pd.read_csv(transaction_file)


    if df_transaction.empty :
        return "There is no transactions yet."
    
    total_sales=df_transaction['transaction_amount'].sum()
    total_quantity_sold=df_transaction['quantity'].sum()
    average_transaction_amount= df_transaction['transaction_amount'].mean()
    total_number_of_transaction=df_transaction['transaction_id'].nunique()
    min_tranasaction_amount=df_transaction['transaction_amount'].min()
    max_transaction_amount=df_transaction['transaction_amount'].max()
    number_of_unique_customers=df_transaction['customer_id'].nunique()
    top1_product= df_transaction.groupby('product_id')['transaction_amount'].sum().sort_values(ascending=False).head(1)
    payment_method=df_transaction['payment_method'].value_counts()
    top1_customer=df_transaction.groupby('customer_id')['transaction_amount'].sum().sort_values(ascending=False).head(1)
    sales_per_store=df_transaction.groupby('store_id')['transaction_amount'].sum().sort_values(ascending=False).head(1)

    report=f"""
        Total Sales = {total_sales:.2f}
        Total Quantity Sold = {total_quantity_sold:.2f}
        Average Transaction Amount = {average_transaction_amount:.2f} 
        Total Number Of Transactions = {total_number_of_transaction}
        Min Transaction Amount = {min_tranasaction_amount:.2f}
        Max Transaction Amount = {max_transaction_amount:.2f}
        Number of Unique Customers = {number_of_unique_customers}
        Top 1 produc sold = {top1_product}
        Sales per Payment Method = {payment_method}
        Top 1 Customer = {top1_customer}
        Top 1 store = {sales_per_store}
    """

    report_file=os.path.join(folder, 'Daily_Report.txt')

    with open(report_file, 'w') as f:
        f.write(report)

    return report_file

def delete_files():
    folder='/opt/airflow/generated_data/'
    for file in os.listdir(folder):
        file_path=os.path.join(folder, file)
        if os.path.isfile(file_path):
            os.remove(file_path)


def send_email():

    conn = BaseHook.get_connection("sendgrid_default")
    api_key = conn.extra_dejson.get("api_key")
    
    message = Mail(
        from_email='ae2637190@gmail.com',        
        to_emails='ae2637190@gmail.com',         
        subject='Daily Metrics Report',
        html_content="Your Daily Metrics Report is nor ready, Check it out!",
    )


    file_path = os.path.join('/opt/airflow/generated_data/', 'Daily_Report.txt')
    
    with open(file_path, 'rb') as f:
        file_data = f.read()
    encoded_file = base64.b64encode(file_data).decode()
    
    attachment = Attachment(
        FileContent(encoded_file),
        FileName('Daily_Report.txt'),
        FileType('text/plain'),
        Disposition('attachment')
    )

    message.attachment = attachment

    
    sg = SendGridAPIClient(api_key)
    response = sg.send(message)


with DAG(
    'Pipeline',
    description='A simple data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args={
            'owner': 'airflow',
            'start_date': days_ago(1),
            'retries': 1,
    }

)as dag:
    
    create_tables_task= PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )
    
    generate_customer_data=PythonOperator(
        task_id='generate_customers_data',
        python_callable= generate_customer_data,

    )

    generate_product_data=PythonOperator(
        task_id='generate_product_data',
        python_callable=generate_product_data,
    )


    generate_stores_data=PythonOperator(
        task_id='generate_stores_data',
        python_callable=generate_stores_data,
    )

    generate_transaction_data=PythonOperator(
        task_id='generate_transaction_data',
        python_callable=generate_transaction_data,
    )

    calculate_metrics=PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics,
        provide_context=True,
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email,
    )

    delete_files=PythonOperator(
        task_id='delete_files',
        python_callable=delete_files,
    )


    create_tables_task>> (generate_customer_data, generate_product_data, generate_stores_data, generate_transaction_data) >> calculate_metrics >> send_email >> delete_files
















