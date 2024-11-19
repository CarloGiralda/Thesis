import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
def create_connection(db_name="./result/utxo/accounts.db"):
    conn = sqlite3.connect(db_name)
    return conn

# Create the accounts table
def create_table(conn):
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                address TEXT PRIMARY KEY,
                balance REAL
            );
        """)
    print("Table 'accounts' created successfully.")

# Insert account records into the table
def insert_account(conn, address, balance):
    with conn:
        conn.execute("INSERT INTO accounts (address, balance) VALUES (?, ?);", (address, balance))

# Insert many account records into the table
def insert_many_accounts(conn, accounts):
    with conn:
        conn.executemany("INSERT INTO accounts (address, balance) VALUES (?, ?);", accounts)

# Retrieve all eligible accounts
def retrieve_eligible_accounts(conn, minimum, maximum):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts WHERE balance BETWEEN ? AND ?;", (minimum, maximum))
    rows = cursor.fetchall()
    return rows

# Retrieve all non-eligible accounts
def retrieve_non_eligible_accounts(conn, minimum, maximum):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts WHERE balance < ? OR balance > ?;", (minimum, maximum))
    rows = cursor.fetchall()
    return rows

# Retrieve all accounts
def retrieve_all_accounts(conn):
    rows = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts;")
    rows = cursor.fetchall()
    return rows