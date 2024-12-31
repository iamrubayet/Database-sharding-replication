import psycopg2

# Connection settings for both Docker-based databases
conn1 = psycopg2.connect(
    host="localhost", port="5433", database="user_data_shard",
    user="root", password="root01"
)

conn2 = psycopg2.connect(
    host="localhost", port="5434", database="user_data_shard",
    user="root", password="root01"
)

def insert_user(user_id, username, email):
    # Simple sharding rule: even user_ids go to conn1, odd to conn2
    conn = conn1 if user_id % 2 == 0 else conn2
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (user_id, username, email) VALUES (%s, %s, %s)", (user_id, username, email))
    conn.commit()
    cursor.close()
    print(f"Inserted {username} into {'primary' if user_id % 2 == 0 else 'secondary'} shard.")

# Test data insertion to verify sharding
for i in range(1, 11):
    username = f"user_{i}"
    email = f"user_{i}@example.com"
    insert_user(i, username, email)

# Fetch and print data from both shards
def fetch_users():
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()
    cursor1.execute("SELECT * FROM users")
    cursor2.execute("SELECT * FROM users")

    print("\nUsers in Primary Shard (5432):")
    for row in cursor1.fetchall():
        print(row)

    print("\nUsers in Secondary Shard (5433):")
    for row in cursor2.fetchall():
        print(row)

    cursor1.close()
    cursor2.close()

fetch_users()

# Close connections
conn1.close()
conn2.close()
