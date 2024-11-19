from utxo_hub.address_extraction import address_extraction
from utxo_hub.utxo_script_conversion import utxo_script_conversion
from utxo_hub.multi_input_address_clustering import multi_input_address_clustering

dir_transactions = './result/utxos/'
dir_sorted_blocks = './result/blocks/'
csv_file = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump.csv'
csv_file_with_addresses = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump_with_addresses.csv'

MULTI_INPUT_ADDRESSES = True

def main():
    utxo_script_conversion(csv_file, csv_file_with_addresses)
    address_extraction(csv_file_with_addresses)
    if MULTI_INPUT_ADDRESSES:
        multi_input_address_clustering(dir_sorted_blocks)

if __name__ == '__main__':
    main()