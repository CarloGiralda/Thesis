import os
from address_extraction import address_extraction
from utxo_reverse import utxo_reverse

dir_transactions = './result/utxos/'
csv_file = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump_856000.csv'

chunk_size = 1000000

def main():
    address_extraction(csv_file, chunk_size)

if __name__ == '__main__':
    main()