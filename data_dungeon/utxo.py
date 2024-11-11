from utxo_hub.address_extraction import address_extraction

dir_transactions = './result/utxos/'
csv_file = '/home/carlo/Documents/PythonProjects/BitcoinParser/result/utxo/utxodump_856003.csv'

def main():
    address_extraction(csv_file)

if __name__ == '__main__':
    main()