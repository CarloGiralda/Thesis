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
        block_hash = file.readline().split(':')[1].strip()
        prev_block_hash = file.readline().split(':')[1].strip()
    return Block(block_num, block_hash, prev_block_hash)

def sort_blocks(blocks):
    """
    Sort blocks based on prev_block_hash and block_hash.
    """
    # Create a mapping from block hash to block for quick lookup
    block_map = {block.block_hash: block for block in blocks}
    
    # Find the first block (the one without a valid prev_block_hash in the set)
    first_block = None
    for block in blocks:
        if block.prev_block_hash not in block_map:
            first_block = block
            break

    if first_block is None:
        raise ValueError("No starting block found (possibly disconnected blocks).")

    # Now sort the blocks using the prev_block_hash -> block_hash linkage
    sorted_blocks = []
    current_block = first_block

    with tqdm(total=len(blocks), desc=f'Sorting blocks') as pbar:
        while current_block:
            sorted_blocks.append(current_block)

            # Remove the current block from the map
            del block_map[current_block.block_hash]

            # Find the next block using the current block's hash
            current_block = block_map.get(current_block.block_hash, None)

            pbar.update(1)

    return sorted_blocks

def process_blocks(directory):
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

    # Rename block files based on sorted order
    with tqdm(total=len(sorted_blocks), desc=f'Renaming blocks') as pbar:
        for idx, block in enumerate(sorted_blocks):
            old_file_name = f"block_{block.block_num}.txt"
            # the 0 is added to prevent the overwriting of the file that previously was block_{idx}
            new_file_name = f"block_0{idx}.txt"
            old_path = os.path.join(directory, old_file_name)
            new_path = os.path.join(directory, new_file_name)
            
            print(f"Renaming {old_file_name} to {new_file_name}")
            os.rename(old_path, new_path)

            pbar.update(1)