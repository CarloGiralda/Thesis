import psycopg
from tqdm import tqdm
from database.config import load_config
from blk_files_parsing.database.sorting_database import Block

class Connection:
    def __init__(self):
        config = load_config()
        self.conn = psycopg.connect(**config)

    def create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                    CREATE TABLE block (
                        block_hash bytea PRIMARY KEY,
                        block_previous_hash bytea,
                        block_num bigint,
                        reward bigint,
                        fees bigint)
                    """)

            cur.execute("""
                CREATE TABLE transaction (
                    transaction_hash bytea PRIMARY KEY,
                    locktime bytea)
                """)

            cur.execute("""
                CREATE TABLE input (
                    input_index serial PRIMARY KEY,
                    transaction_hash bytea REFERENCES transaction(transaction_hash),
                    previous_transaction_hash bytea,
                    previous_transaction_index bytea,
                    sender bytea,
                    sequence_number bigint)
                """)

            cur.execute("""
                CREATE TABLE output (
                    output_index serial PRIMARY KEY,
                    transaction_hash bytea REFERENCES transaction(transaction_hash),
                    value bigint,
                    receiver bytea)
                """)
        
        self.conn.commit()
    
    def populate_tables(self, blocks):
        with self.conn.cursor() as cur:
        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no SQL injections!)
            for block_index in tqdm(range(len(blocks)), desc='Filling database'):
                for transaction_index in range(len(blocks[block_index])):
                    if transaction_index == 0:
                        cur.execute(
                            "INSERT INTO block (block_hash, block_previous_hash, block_num, reward, fees) VALUES (%s, %s, %s, %s, %s)",
                            (blocks[block_index][transaction_index][0].split(':')[1].strip(), 
                            blocks[block_index][transaction_index][1].split(':')[1].strip(), 
                            block_index, 
                            blocks[block_index][transaction_index][2].split(':')[1].strip(), 
                            blocks[block_index][transaction_index][3].split(':')[1].strip()))

                    else:
                        cur.execute(
                            "INSERT INTO transaction (transaction_hash, locktime) VALUES (%s, %s)",
                            (blocks[block_index][transaction_index][-1].split(':')[1].strip(), 
                            blocks[block_index][transaction_index][-2].split(':')[1].strip()))
                        
                        inputs_count = int(blocks[block_index][transaction_index][0].split(':')[1].strip(), 10)
                        for input_index in range(0, inputs_count):
                            cur.execute(
                                "INSERT INTO input (transaction_hash, previous_transaction_hash, previous_transaction_index, sender, sequence_number) VALUES (%s, %s, %s, %s, %s)",
                                (blocks[block_index][transaction_index][-1].split(':')[1].strip(), 
                                blocks[block_index][transaction_index][input_index * 4 + 1].split(':')[1].strip(),
                                blocks[block_index][transaction_index][input_index * 4 + 2].split(':')[1].strip(),
                                blocks[block_index][transaction_index][input_index * 4 + 3].split(':')[1].strip(),
                                blocks[block_index][transaction_index][input_index * 4 + 4].split(':')[1].strip()))

                        starting_output_index = inputs_count * 4 + 1
                        outputs_count = int(blocks[block_index][transaction_index][starting_output_index].split(':')[1].strip(), 10)
                        for output_index in range(0, outputs_count):
                            cur.execute(
                                "INSERT INTO output (transaction_hash, value, receiver) VALUES (%s, %s, %s)",
                                (blocks[block_index][transaction_index][-1].split(':')[1].strip(), 
                                blocks[block_index][transaction_index][starting_output_index + output_index * 2 + 1].split(':')[1].strip(),
                                blocks[block_index][transaction_index][starting_output_index + output_index * 2 + 2].split(':')[1].strip()))
        
        self.conn.commit()

    def retrieve_blocks(self):
        blocks = []

        with self.conn.cursor() as cur:
            cur.execute("SELECT  block_hash, prev_block_hash FROM bitcoin.block")

            response = cur.fetchone()

            while response != None:
                current_block = Block(response.block_hash, response.prev_block_hash)
                blocks.append(current_block)

                response = cur.fetchone()
        
        return blocks
    
    def update_blocks(self, sorted_blocks):
        with self.conn.cursor() as cur:
            for index, block in enumerate(sorted_blocks):
                cur.execute("UPDATE bitcoin.block SET block_num=%s WHERE block_hash=%s",
                            index, block.block_hash)
        self.conn.commit()