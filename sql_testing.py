import psycopg2
import sys
import boto3

ENDPOINT="ski-db.c7wlbjagdmho.eu-west-2.rds.amazonaws.com"
PORT="5432"
USR="marc"
REGION="eu-west-2a"
DBNAME="ski-db"

#gets the credentials from .aws/credentials
session = boto3.Session(profile_name='marc')
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)

try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=token)
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e)) 