from tqdm import tqdm

class Block:
    def __init__(self, block_hash, prev_block_hash):
        self.block_hash = block_hash
        self.prev_block_hash = prev_block_hash

def sort_blocks(blocks):
    """
    Sort blocks based on prev_block_hash and block_hash.
    """
    # Create a mapping from block hash to block for quick lookup
    block_map = {block.prev_block_hash: block for block in blocks}
    
    first_block = Block(block_hash='00000000000000000001e5e0ef35fd43fd03dfb2ccbefa94d2e44da62547d3e4',
                        prev_block_hash='000000000000000000025fad77734a01521a0e03c2b50feb9c1d259f62f48296')

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

def process_blocks(conn):
    blocks = conn.retrieve_blocks()

    # Sort blocks by prev_block_hash -> block_hash
    sorted_blocks = sort_blocks(blocks)

    conn.update_blocks(sorted_blocks)