import psycopg2
from time import sleep

# Connection to the primary database
primary_conn = psycopg2.connect(
    host="localhost", port="5432", database="replication_demo",
    user="root", password="root01"
)

# Connection to the replica database
replica_conn = psycopg2.connect(
    host="localhost", port="5433", database="replication_demo",
    user="root", password="root01"
)

def insert_data(name):
    cursor = primary_conn.cursor()
    cursor.execute("INSERT INTO test_table (name) VALUES (%s)", (name,))
    primary_conn.commit()
    cursor.close()
    print(f"Inserted {name} into primary database.")

def fetch_data():
    cursor = replica_conn.cursor()
    cursor.execute("SELECT * FROM test_table")
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Insert data into the primary and verify it's in the replica
insert_data("David")
insert_data("Eve")

# Give replication a moment to catch up
sleep(2)

# Fetch data from the replica
print("\nData in Replica Database:")
for row in fetch_data():
    print(row)

# Close connections
primary_conn.close()
replica_conn.close()
