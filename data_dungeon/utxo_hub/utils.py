import pandas as pd

# convertion of a CSV file to an HDF5 file in chunks to save memory
def csv_to_hdf(csv_file, hdf_file, data_columns, hdf_key='data', chunksize=100000):
    with pd.HDFStore(hdf_file, mode='w') as store:
        for chunk in pd.read_csv(csv_file, chunksize=chunksize):
            # index on 'txid'
            store.append(hdf_key, chunk, data_columns=data_columns)

def open_hdf_for_operations(hdf_file, batch_size = 100, hdf_key='data'):
    store = pd.HDFStore(hdf_file, mode='a')  # Open in append mode

    def delete_rows_in_batches_by_txid(txids):
        for i in range(0, len(txids), batch_size):
            batch_txids = txids[i:i + batch_size]
            query = ' | '.join([f'txid == {repr(txid)}' for txid in batch_txids])
            store.remove('data', where=query)

    def add_rows_with_txid(data):
        # Append the data to the HDF file; `data_columns` ensures 'txid' remains indexed.
        store.append(hdf_key, data, data_columns=['txid', 'address'])

    return store, delete_rows_in_batches_by_txid, add_rows_with_txid