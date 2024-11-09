import pandas as pd
import time
from data_dungeon.database.database import create_connection, create_table, insert_many_accounts

def address_extraction(csv_file, chunk_size):
    addresses = {}

    with pd.read_csv(csv_file, chunksize=chunk_size) as reader:
        start_time  = time.time()

        for index, chunk in enumerate(reader):
            for _, row in chunk.iterrows():
                address = row['address']
                amount = row['amount']
                if not address in addresses:
                    addresses[address] = amount
                else:
                    addresses[address] += amount

            end_time  = time.time()
            print(f'Processed rows: {(index + 1) * chunk_size}')
            print(f'Total addresses: {len(addresses)}')
            print(f'Time elapsed (in seconds): {end_time - start_time}')

    accounts = list(addresses.items())

    start_time  = time.time()
    # create the database, create the table, populate the table
    conn = create_connection()
    create_table(conn)
    insert_many_accounts(conn, accounts)
    end_time  = time.time()
    print(f'Time elapsed (in seconds): {end_time - start_time}')