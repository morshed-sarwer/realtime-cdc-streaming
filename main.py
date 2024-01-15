from datetime import datetime
import random
from faker import Faker
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='cdc',
    user='postgres',
    password='123'
)
cursor = conn.cursor()


def generate_fake_data():
    faker = Faker()
    return {
        'name': faker.name(),
        'email': faker.email(),
        'address': faker.address(),
        'salary': round(random.uniform(0, 100), 2),
        'joining_date': datetime.now(),
        'designation': random.choice(['Data Engineer'])
    }


def create_table():
    cursor.execute(
        """
     CREATE TABLE IF NOT EXISTS employee(
     id SERIAL PRIMARY KEY,
     name varchar(100),
     email varchar(100),
     address varchar(100),
     salary decimal,
     joining_date timestamp,
     designation varchar(100)
     )
    """
    )


def insert_data():
    create_table()
    for i in range(100):
        data = generate_fake_data()
        insert_query = sql.SQL("INSERT INTO employee(name, email, address, salary, joining_date, designation) VALUES(%s,"
                               "%s,%s,%s,%s,%s)")
        email = data['joining_date']
        print(email)
        cursor.execute(insert_query, (
            data['name'], data['email'], data['address'], data['salary'], data['joining_date'], data['designation']))
    cursor.close()
    conn.commit()

insert_data()
