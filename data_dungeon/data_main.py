import os
import plyvel
from file_parsing.rev_parser import block_undo, get_block
from file_parsing.index_parser import block_index, format_hash
from file_parsing.blk_parser import block_parsing
from file_parsing.blk_parser_utxo import block_parsing_utxo
from tqdm import tqdm

dir_blocks = '/home/carlo/.bitcoin/blocks/' # Directory where blk*.dat files are stored
dir_indexes = '/home/carlo/.bitcoin/blocks/index'
dir_results_blocks = './result/blocks/' # Directory where to save blocks results
dir_results_utxos = './result/utxos/' # Directory where to save utxos results

EXTRACT_BLOCKS = True  # Flag to determine whether blocks should be extracted by the parser to be be processed
EXTRACT_TRANSACTIONS = False  # Flag to determine whether transactions should be extracted by the parser to be used to revert utxos up to a selected block
LIST_BLOCKS = False  # Flag to determine whether blocks' general information should be displayed
start_block = 856000  # used by EXTRACT_BLOCKS, EXTRACT_TRANSACTIONS, LIST_BLOCKS
end_block = 866000  # used only by EXTRACT_BLOCKS (EXTRACT_TRANSACTIONS automatically sets the end block to te last block available, LIST_BLOCKS' purpose is to list all subsequent values)

def delete_last_block(dir_results_blocks):
    files = [f for f in os.listdir(dir_results_blocks)]
    files.sort()
    del_file = os.path.join(dir_results_blocks, files[-1])
    os.remove(del_file)
    print(f'{del_file} (last block) deleted')

def main():
    with plyvel.DB(dir_indexes, compression=None) as db:
        # Block index entries are stored with keys prefixed by 'b'
        with db.iterator(prefix=b'b') as iterator:
            blockIndexes = [block_index(format_hash(k[1:]), v) for k, v in iterator]
            blockIndexes.sort(key=lambda x: x[0])

    # blocks are extracted by the parser
    if EXTRACT_BLOCKS:

        if os.path.exists(dir_results_blocks) and len(os.listdir(dir_results_blocks)) > 0:
            # delete the last block that could have been compromised
            delete_last_block(dir_results_blocks)

        with tqdm(range(start_block, end_block + 1), desc='Writing blocks') as pbar:
            for block in blockIndexes:
                # block = (height, file, data_pos, undo_pos)
                if start_block <= block[0] <= end_block:
                    block_path = dir_results_blocks + f'block_{block[0]}.txt'

                    if not os.path.exists(block_path):
                        blk_path = os.path.join(dir_blocks, f'blk0{block[1]}.dat')
                        rev_path = os.path.join(dir_blocks, f'rev0{block[1]}.dat')
                        actual_block = block_parsing(blk_path, block[2] - 8)
                        undo_block = block_undo(get_block(rev_path, block[3] - 8))

                        for transaction_index in range(len(undo_block)):
                            for output_index in range(len(undo_block[transaction_index])):
                                # transaction_index + 1 because in the undo_block the coinbase transaction is not present
                                if actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Sender'] == None and undo_block[transaction_index][output_index]['Script'] != 'Unknown':
                                    actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Sender'] = undo_block[transaction_index][output_index]['Address']
                                # always set the value
                                actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Value'] = undo_block[transaction_index][output_index]['Amount']
                        
                        with open(block_path,'w+') as f:
                            f.write(str(actual_block))

                    pbar.update(1)
    
    # blocks' general information are displayed
    if LIST_BLOCKS:
        for block in blockIndexes:
            if start_block <= block[0]:
                print(block)

    # transactions are extracted by the parser to be used to revert utxos up to a selected block
    if EXTRACT_TRANSACTIONS:
        # height of the last block
        last_block = blockIndexes[-1][0]
        with tqdm(range(start_block, last_block + 1), desc='Writing blocks') as pbar:
            for block in blockIndexes:
                # block = (height, file, data_pos, undo_pos)
                if 856000 <= block[0]:
                    block_path = dir_results_utxos + f'block_{block[0]}.txt'

                    if not os.path.exists(block_path):
                        blk_path = os.path.join(dir_blocks, f'blk0{block[1]}.dat')
                        rev_path = os.path.join(dir_blocks, f'rev0{block[1]}.dat')
                        actual_block = block_parsing_utxo(blk_path, block[2] - 8)
                        undo_block = block_undo(get_block(rev_path, block[3] - 8))

                        for transaction_index in range(len(undo_block)):
                            for output_index in range(len(undo_block[transaction_index])):
                                # transaction_index + 1 because in the undo_block the coinbase transaction is not present
                                if actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Sender'] == None and undo_block[transaction_index][output_index]['Script'] != 'Unknown':
                                    actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Sender'] = undo_block[transaction_index][output_index]['Address']
                                # always set the value
                                actual_block['Transactions'][transaction_index + 1]['Inputs'][output_index]['Value'] = undo_block[transaction_index][output_index]['Amount']
                        
                        with open(block_path,'w+') as f:
                            f.write(str(actual_block))

                    pbar.update(1)

if __name__ == '__main__':
    main()