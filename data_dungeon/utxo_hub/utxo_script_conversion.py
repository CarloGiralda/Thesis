import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from utils.parse_transaction_output import parse_transaction_output

def _process_row(row):
    script = row['script']
    script_type, address = parse_transaction_output(script)
    if not isinstance(address, list):
        row['address'] = address

    return row

def _process_chunk(chunk):
    criterion = chunk['address'].map(lambda x: not str(x).startswith('bc1') and not str(x).startswith('1') and not str(x).startswith('3'))
    selected_rows = chunk[criterion]

    chunk.loc[selected_rows.index] = selected_rows.apply(_process_row, axis=1)

    return chunk
    
def utxo_script_conversion(input_csv, output_csv, chunk_size=1000000):
    with open(output_csv, 'w+') as f_out:
        first_chunk = True  # To manage headers only once
        
        with ProcessPoolExecutor() as executor:
            for chunk in tqdm(pd.read_csv(input_csv, chunksize=chunk_size), desc='Processing CSV'):
                # Submit each chunk for processing immediately
                future = executor.submit(_process_chunk, chunk)
                
                # Write the processed chunk once done
                result_chunk = future.result()
                result_chunk.to_csv(f_out, mode='a', index=False, header=first_chunk)
                
                # Only include header for the first chunk written
                first_chunk = False