import os
import plyvel
from file_parsing.rev_parser import block_undo, get_block
from file_parsing.index_parser import block_index, format_hash
from file_parsing.blk_parser import transaction_parsing
from tqdm import tqdm

dir_blocks = '/home/carlo/.bitcoin/blocks/' # Directory where blk*.dat files are stored
dir_indexes = '/home/carlo/.bitcoin/blocks/index'
dir_results_blocks = './result/blocks/' # Directory where to save blocks results

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

    if os.path.exists(dir_results_blocks) and len(os.listdir(dir_results_blocks)) > 0:
        # delete the last block that could have been compromised
        delete_last_block(dir_results_blocks)

    with tqdm(range(856000, 866001), desc='Writing blocks') as pbar:
        for block in blockIndexes:
            # block = (height, file, data_pos, undo_pos)
            if 856000 <= block[0] <= 866000:
                block_path = dir_results_blocks + f'block_{block[0]}.txt'

                if not os.path.exists(block_path):
                    blk_path = os.path.join(dir_blocks, f'blk0{block[1]}.dat')
                    rev_path = os.path.join(dir_blocks, f'rev0{block[1]}.dat')
                    actual_block = transaction_parsing(blk_path, block[2] - 8)
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