import struct
import os
from utils.utils import read_varint, read_bytes

# Compact serialization special case constants
P2PKH_PREFIX = b'\x76\xa9\x14'  # OP_DUP OP_HASH160 20-byte key hash OP_EQUALVERIFY OP_CHECKSIG
P2SH_PREFIX = b'\xa9\x14'       # OP_HASH160 20-byte script hash OP_EQUAL
P2PK_COMPRESSED_PREFIX = [b'\x02', b'\x03']  # Compressed public keys (33 bytes)
P2PK_UNCOMPRESSED_PREFIX = b'\x04'           # Uncompressed public key (65 bytes)

def decompress_amount(x):
    if x == 0:
        return 0
    x -= 1
    e = x % 10
    x /= 10
    n = 0
    if e < 9:
        d = (x % 9) + 1
        x /= 9
        n = x * 10 + d
    else:
        n = x + 1
    while e:
        n *= 10
        e -= 1
    return n

def decompress_script(file):
    pos = file.tell()
    b = file.read(1)
    bInt = int(b.hex(), 16)

    if bInt == 0:
        script = read_bytes(file, 20)
        return "P2PKH", script
    elif bInt == 1:
        script = read_bytes(file, 20)
        return "P2SH", script
    elif bInt == 2 or bInt == 3:
        script = read_bytes(file, 32)
        return "P2PK (compressed)", script
    elif bInt == 4 or bInt == 5:
        script = read_bytes(file, 64)
        return "P2PK (uncompressed)", script

    file.seek(pos)

    script_length = int(read_varint(file), 16) - 6
    script = read_bytes(file, script_length)

    # If no special case matched, return raw script
    return "Raw script", script

def parse_tx_undo(file):
    """Parse a transaction undo record."""
    num_inputs = int(read_varint(file), 16)  # Number of inputs undone
    print(num_inputs)
    inputs = []
    
    for i in range(num_inputs):
        # Read height
        height = int(read_varint(file), 16)

        # Read version
        version = int(read_varint(file), 16)

        # Read amount (satoshis)
        amount = int(read_varint(file), 16)
        amount = decompress_amount(amount)
        
        # Read script length and script
        script_type, script = decompress_script(file)
        
        inputs.append({
            'height': height,
            'version': version,
            'amount': amount,
            'script_type': script_type,
            'script': script
        })
    
    return inputs

def parse_rev_file(file_path):
    """Parse a Bitcoin revxxxx.dat file."""
    with open(file_path, 'rb') as f:
        file_size = os.path.getsize(file_path)

        while f.tell() != file_size:
            magic_number = read_bytes(f, 4)
            size = read_bytes(f, 4)

            # Attempt to read the number of transactions with undo data
            tx_undo_count = int(read_varint(f), 16)  # Number of transactions with undo data
            tx_undo_list = []

            for i in range(tx_undo_count):
                tx_undo = parse_tx_undo(f)
                print(tx_undo)
                tx_undo_list.append(tx_undo)

            checksum = read_bytes(f, 32)
            
            print(tx_undo_list)
            print(f"Parsed {tx_undo_count} transaction undo records.")