from utxo_hub.address_extraction import address_extraction
from utxo_hub.utxo_script_conversion import utxo_script_conversion

dir_transactions = './result/utxos/'
csv_file = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump.csv'
csv_file_with_addresses = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump_with_addresses.csv'

def main():
    utxo_script_conversion(csv_file, csv_file_with_addresses)
    address_extraction(csv_file_with_addresses)

if __name__ == '__main__':
    main()