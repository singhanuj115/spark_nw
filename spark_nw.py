import requests
import os
import pandas as pd
import psycopg2.extras as extras

from dateutil.parser import parse
from sqlalchemy import create_engine


def execute_create_queries_on_postgres():
    conn_str = "postgresql://root:spark_nw_pw@localhost:5433/postgres"
    engine = create_engine(conn_str)
    connection = engine.connect()

    create_users_sql = '''CREATE TABLE USERS(
                        date TIMESTAMP,
                        name CHAR(20) NOT NULL,
                        age INT,
                        city CHAR(150),
                        country CHAR(150),
                        emailDomain CHAR(150),
                        gender CHAR(150),
                        smoking CHAR(150),
                        income INT
                        )'''

    create_subscription_sql = '''
                             CREATE TABLE SUBSCRIPTION(
                            id INT,
                            createdAt TIMESTAMP,
                            updatedAt TIMESTAMP,
                            status CHAR(150),
                            )
                            '''

    create_messages_sql = '''
                          CREATE TABLE MESSAGE(
                          createdAt TIMESTAMP,
                          receiverId INT,
                          senderId INT
                          )
                          '''

    connection.execute(create_users_sql)
    connection.execute(create_subscription_sql)
    connection.execute(create_messages_sql)


def extract():
    users_data = requests.get("https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users")
    users_data = users_data.json()
    users_data = pd.DataFrame(users_data)

    messages_data = requests.get("https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages")
    messages_data = messages_data.json()
    messages_data = pd.DataFrame(messages_data)

    return users_data, messages_data


def get_age(birth_date):
    birth_date = parse(birth_date)
    today = date.today()
    age = today.year - birth_date.year
    return age


def get_gender(profile):
    return profile['gender']


def get_smoking(profile):
    return profile['isSmoking']


def get_income(profile):
    return profile['income']


def get_email(email):
    return email.split("@")[-1]


def get_subscription_status(subscription):
    try:
        return subscription[0]['status']
    except:
        return None


def transform(users_data, messages_data):
    modified_users_data = pd.DataFrame()
    modified_users_data['createdAt'] = users_data['createdAt']
    modified_users_data['age'] = users_data.apply(lambda x: get_age(x['birthDate']), axis=1)
    modified_users_data['city'] = users_data['city']
    modified_users_data['country'] = users_data['country']
    modified_users_data['emailDomain'] = users_data.apply(lambda x: get_email(x['email']), axis=1)
    modified_users_data['gender'] = users_data.apply(lambda x: get_gender(x['profile']), axis=1)
    modified_users_data['smoking'] = users_data.apply(lambda x: get_smoking(x['profile']), axis=1)
    modified_users_data['income'] = users_data.apply(lambda x: get_income(x['profile']), axis=1)
    modified_users_data['updatedAt'] = users_data['updatedAt']

    subscription_users_data = pd.DataFrame()
    subscription_users_data['id'] = users_data['id']
    subscription_users_data['createdAt'] = users_data['createdAt']
    subscription_users_data['updatedAt'] = users_data['updatedAt']
    subscription_users_data['status'] = users_data.apply(lambda x: get_subscription_status(x['subscription']), axis=1)

    messages = messages_data[['createdAt', 'receiverId', 'senderId']]

    return modified_users_data, subscription_users_data, messages


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def load(modified_users_data, subscription_users_data, messages):
    conn_str = "postgresql://root:spark_nw_pw@localhost:5433/spark_nw"
    engine = create_engine(conn_str)
    connection = engine.raw_connection()
    execute_values(connection, modified_users_data, 'users')
    execute_values(connection, subscription_users_data, 'subscription')
    execute_values(connection, messages, 'message')
    connection.commit()


def main():
    # execute_create_queries_on_postgres()
    users_data, messages_data = extract()
    modified_users_data, subscription_users_data, messages = transform(users_data, messages_data)
    load(modified_users_data, subscription_users_data, messages)


if __name__ == "__main__":
    main()
