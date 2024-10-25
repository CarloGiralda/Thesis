import os
from block_parsing.block_parser import block_parsing
from transaction_parsing.transaction_parser import transaction_parsing
from transaction_parsing.sorting_files import process_blocks
from tqdm import tqdm

# BLOCKS parses the blk*.dat files to obtain blocks that contain all information
BLOCKS = False
# TRANSACTIONS is a more lightweight version of BLOCKS, where only information related to transactions are kept
TRANSACTIONS = True

dir_blocks = '/home/carlo/.bitcoin/blocks/' # Directory where blk*.dat files are stored
dir_results_blocks = './result/blocks/' # Directory where to save blocks results
dir_results_transactions = './result/transactions/' # Directory where to save transactions results
dir_results_sorted_transactions = './result/sorted_transactions/' # Directory where to save sorted transactions results

def blocks(files):
    general_counter = 0

    for file_dat in files:
        blocks = []

        # path of the current block
        t = dir_blocks + file_dat

        with open(t,'rb') as f:
            file_size = os.path.getsize(t)

            with tqdm(total=file_size, desc=f'Processing blocks of file {t}') as pbar:
                # if the current file position matches the end of the file, then we reached EOF
                while f.tell() != file_size:
                    block = block_parsing(f)

                    blocks.append(block)

                    pbar.n = f.tell()
                    pbar.refresh()
        
        if not os.path.exists(dir_results_blocks):
            os.makedirs(dir_results_blocks)

        for j in tqdm(range(len(blocks)), desc='Writing transactions'):
            with open(dir_results_blocks + f'block_{general_counter + j}.txt','w+') as f:
                f.write(str(blocks[j]))

        general_counter += len(blocks)

def transactions(files):
    general_counter = 0
    parsed_blocks = []

    dir_parsed_blocks = dir_results_transactions + 'list_of_parsed_blocks.txt'
    file_size = os.path.getsize(dir_parsed_blocks)
    with open(dir_parsed_blocks, 'r') as file:
        while file.tell() != file_size:
            parsed_block = file.readline().strip()
            parsed_blocks.append(parsed_block)

    for file_dat in files:
        # skip if the file has already been processed
        if file_dat in parsed_blocks:
            print(f'Skip {file_dat} file')
            continue

        blocks = []

        # path of the current block
        t = dir_blocks + file_dat

        with open(t,'rb') as f:
            file_size = os.path.getsize(t)

            with tqdm(total=file_size, desc=f'Processing blocks of file {t}') as pbar:
                # if the current file position matches the end of the file, then we reached EOF
                while f.tell() != file_size:
                    block = transaction_parsing(f)

                    if block is not None:
                        blocks.append(block)

                    pbar.n = f.tell()
                    pbar.refresh()
        
        if not os.path.exists(dir_results_transactions):
            os.makedirs(dir_results_transactions)
    
        for i in tqdm(range(len(blocks)), desc='Writing transactions'):
            with open(dir_results_transactions + f'block_{general_counter + i}.txt','w+') as f:
                for j in blocks[i]:
                    for k in j:
                        f.write(str(k) + '\n')
                    f.write('\n')

        general_counter += len(blocks)

        with open(dir_parsed_blocks, 'a+') as file:
            file.write(file_dat + '\n')


    # BLOCK_DOWNLOAD_WINDOW = 1024 by default
    process_blocks(dir_results_transactions, dir_results_sorted_transactions)

def main():
    files = os.listdir(dir_blocks)
    files = [x for x in files if (x.endswith('.dat') and x.startswith('blk'))]
    files.sort()

    if BLOCKS:
        blocks(files)
    if TRANSACTIONS:
        transactions(files)

if __name__ == '__main__':
    main()
