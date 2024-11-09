import os
import ast
import pandas as pd
import time
from tqdm import tqdm

def utxo_reverse(dir_transactions, addresses_file):
    files = [f for f in os.listdir(dir_transactions)]
    # reverse ordering
    files.sort(reverse=True)

    with tqdm(total=len(files), desc='Reversing utxos') as pbar, open(addresses_file, 'r') as reader:
        block_str = reader.readline()
        addresses = ast.literal_eval(block_str)

        for file in files:
            start_time = time.time()
            # for each file, transform it into a dictionary
            path = os.path.join(dir_transactions, file)

            with open(path, 'r') as transaction_file:
                block_str = transaction_file.readline()
                transactions = ast.literal_eval(block_str)['Transactions']
                for transaction in transactions:
                    for input in transaction['Inputs']:
                        if input['Sender'] in addresses:
                            addresses[input['Sender']] += input['Value']
                        else:
                            addresses[input['Sender']] = input['Value']
                    for output in transaction['Outputs']:
                        if output['Receiver'] in addresses:
                            addresses[output['Receiver']] -= output['Value']
                        else:
                            addresses[output['Receiver']] = output['Value']
            
            end_time = time.time()
            print(f'File {file} processed in {end_time - start_time} seconds')

            print(len(addresses))

            pbar.update(1)

            break
                
