import sqlite3
import sys

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

def retrieve_address(conn, address):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts WHERE address = ?;", (address,))
    account = cursor.fetchone()
    if account == None:
        return address, 0
    return account[0], account[1]

# Retrieve all accounts
def retrieve_eligible_addresses(conn, minimum, maximum):
    cursor = conn.cursor()
    cursor.execute("SELECT address FROM accounts WHERE balance BETWEEN ? AND ?;", (minimum, maximum))
    rows = cursor.fetchall()
    return rows

# Retrieve all accounts
def retrieve_all_accounts(conn):
    rows = []
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM accounts;")
    while True:
        rows = cursor.fetchmany(size=1000000)
        if len(rows) == 0:
            break
        yield rows

# Update balance for a specific address
def update_balance(conn, address, new_balance):
    with conn:
        conn.execute("UPDATE accounts SET balance = ? WHERE address = ?;", (new_balance, address))