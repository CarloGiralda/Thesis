from tqdm import tqdm
import os

class Block:
    def __init__(self, block_num, block_hash, prev_block_hash):
        self.block_num = block_num
        self.block_hash = block_hash
        self.prev_block_hash = prev_block_hash

def read_block(file_path, block_num):
    """
    Reads a block file and returns a Block object.
    Each file contains the block_hash and prev_block_hash, separated by a newline.
    """
    with open(file_path, 'r') as file:
        block_hash = file.readline().split(':')[1].strip().upper()
        prev_block_hash = file.readline().split(':')[1].strip().upper()
    return Block(block_num, block_hash, prev_block_hash)

def sort_blocks(blocks):
    """
    Sort blocks based on prev_block_hash and block_hash.
    """
    # Create a mapping from block hash to block for quick lookup
    block_map = {block.prev_block_hash: block for block in blocks}
    print(list(block_map.keys())[1024])

    first_block = block_map['000000000000000000025fad77734a01521a0e03c2b50feb9c1d259f62f48296'.upper()]

    # Now sort the blocks using the prev_block_hash -> block_hash linkage
    sorted_blocks = []
    current_block = first_block

    with tqdm(total=len(blocks), desc=f'Sorting blocks') as pbar:
        while current_block:
            sorted_blocks.append(current_block)

            # Remove the current block from the map
            del block_map[current_block.prev_block_hash]

            # Find the next block using the current block's hash
            current_block = block_map.get(current_block.block_hash, None)
            
            pbar.update(1)

    return sorted_blocks

def process_blocks(directory, destination_directory):
    """
    Process all block files in the directory and sort them by linkage.
    Then rename the files based on the sorted order.
    """
    block_files = [f for f in os.listdir(directory) if f.startswith("block_") and f.endswith(".txt")]
    
    # Read all blocks from files
    blocks = []
    with tqdm(total=len(block_files), desc=f'Reading blocks') as pbar:
        for block_file in block_files:
            block_num = int(block_file.split('_')[1].split('.')[0])  # Extract block number
            block_path = os.path.join(directory, block_file)
            block = read_block(block_path, block_num)
            blocks.append(block)

            pbar.update(1)

    # Sort blocks by prev_block_hash -> block_hash
    sorted_blocks = sort_blocks(blocks)

    # Rewriting the ordered blocks to new files
    with tqdm(total=len(sorted_blocks), desc=f'Rewriting blocks') as pbar:
        for idx, block in enumerate(sorted_blocks):
            old_file_name = f"block_{block.block_num}.txt"
            new_file_name = f"block_{idx}.txt"
            old_path = os.path.join(directory, old_file_name)
            new_path = os.path.join(destination_directory, new_file_name)

            file_size = os.path.getsize(old_path)

            with open(old_path, 'r') as rf:
                with open(new_path, 'w') as wf:
                    while rf.tell() != file_size:
                        line = rf.readline()
                        wf.write(line)

            pbar.update(1)
