import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
def create_connection(db_name="./result/utxo/multi_input_accounts.db"):
    conn = sqlite3.connect(db_name)
    return conn

# Create the accounts table
def create_tables(conn):
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS addresses (
                address TEXT PRIMARY KEY,
                user REAL
            );
        """)
    print("Table 'addresses' created successfully.")

    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                user REAL PRIMARY KEY,
                balance REAL
            );
        """)
    print("Table 'accounts' created successfully.")

# Insert many account records into the table
def insert_many_addresses(conn, addresses):
    with conn:
        conn.executemany("INSERT INTO addresses (address, user) VALUES (?, ?);", addresses)

# Insert many account records into the table
def insert_many_accounts(conn, accounts):
    with conn:
        conn.executemany("INSERT INTO accounts (user, balance) VALUES (?, ?);", accounts)

# Retrieve all eligible accounts
def retrieve_eligible_accounts(conn, minimum, maximum):
    cursor = conn.cursor()
    cursor.execute("SELECT addresses.address, accounts.user, accounts.balance FROM accounts JOIN addresses ON accounts.user = addresses.user WHERE accounts.balance BETWEEN ? AND ?;", (minimum, maximum))
    rows = cursor.fetchall()
    return rows

# Retrieve all non-eligible accounts
def retrieve_non_eligible_accounts(conn, minimum, maximum):
    cursor = conn.cursor()
    cursor.execute("SELECT addresses.address, accounts.user, accounts.balance FROM accounts JOIN addresses ON accounts.user = addresses.user WHERE accounts.balance < ? OR balance > ?;", (minimum, maximum))
    rows = cursor.fetchall()
    return rows

# Retrieve all accounts
def retrieve_all_accounts(conn):
    rows = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts JOIN addresses ON accounts.user = addresses.user;")
    rows = cursor.fetchall()
    return rows

def retrieve_user_from_address(conn, address):
    rows = []
    cursor = conn.cursor()
    cursor.execute("SELECT user FROM addresses WHERE address = ?;", (address,))
    rows = cursor.fetchall()
    return rows