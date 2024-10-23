import os
import datetime
from block_parsing.block_parser import block_parsing
from transaction_parsing.transaction_parser import transaction_parsing
from utils.sorting import process_blocks
from tqdm import tqdm

BLOCKS = True
TRANSACTIONS = True
FILE = True
DATABASE = True

dir_blocks = './blocks/' # Directory where blk*.dat files are stored
dir_results_blocks = './result/blocks/' # Directory where to save blocks results
dir_results_transactions = './result/transactions/' # Directory where to save transactions results

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

    # BLOCK_DOWNLOAD_WINDOW = 1024 by default
    #process_blocks(dir_results_transactions)

def transactions(files):
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
                    block = transaction_parsing(f)

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

    # BLOCK_DOWNLOAD_WINDOW = 1024 by default
    #process_blocks(dir_results_transactions)

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
