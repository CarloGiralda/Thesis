import struct
import mmap
from utils.utils import decode_varint, decode_compactsize, decompress_txout_amt, p2pkh_to_address, p2sh_to_address
from utils.parse_transaction_output import parse_transaction_output
from utils.address_conversion import input_script_to_addr

NSPECIALSCRIPTS = 6
# Constant separating blocks in the .blk files
BITCOIN_CONSTANT = b"\xf9\xbe\xb4\xd9"

def decompress_script(raw_hex):
    script_type = raw_hex[0]
    compressed_script = raw_hex[1:]

    script_type_str, address = None, None

    if script_type == 0:
        if len(compressed_script) != 20:
            print('Compressed script has wrong size:')
            print(f'actual size: {len(compressed_script)}')
            print('expected size: 20')
            return None, None
        script_type_str = 'P2PKH'
        address = p2pkh_to_address(compressed_script)

    elif script_type == 1:
        if len(compressed_script) != 20:
            print('Compressed script has wrong size:')
            print(f'actual size: {len(compressed_script)}')
            print('expected size: 20')
            return None, None
        script_type_str = 'P2SH'
        address = p2sh_to_address(compressed_script)

    elif script_type in [2, 3]:
        if len(compressed_script) != 33:
            print('Compressed script has wrong size:')
            print(f'actual size: {len(compressed_script)}')
            print('expected size: 33')
            return None, None
        script_type_str = 'P2PK'
        address = input_script_to_addr(compressed_script, 'P2PKH Input')
        
    elif script_type in [4, 5]:
        if len(compressed_script) != 33:
            print('Compressed script has wrong size:')
            print(f'actual size: {len(compressed_script)}')
            print('expected size: 33')
            return None, None
        prefix = format(script_type - 2, '02')
        compressed_script = prefix + compressed_script[2:]

        script_type_str = 'P2PK'
        address = input_script_to_addr(compressed_script, 'P2PKH Input')
    
    else:
        compressed_script = compressed_script.hex()
        script_type_str, address = parse_transaction_output(compressed_script)

    return script_type_str, address

def block_undo(raw_hex):
    spends = []
    # num_txs is the actual number of transactions inside the block
    # pos is the bytes read by decode_compactsize
    num_txs, pos = decode_compactsize(raw_hex)
    for i in range(num_txs):
        txn, tx_len = spent_transaction(raw_hex=raw_hex[pos:])
        spends.append(txn)
        pos += tx_len

    return spends

def spent_transaction(raw_hex):
    outputs = []
    # output_len is the length of the output
    output_len, pos = decode_compactsize(raw_hex)
    for i in range(output_len):
        output, output_len = spent_output(raw_hex=raw_hex[pos:])
        outputs.append(output)
        pos += output_len
    tx_len = pos

    return outputs, tx_len

def spent_output(raw_hex):
    pos = 0

    # decode height code
    height_code, height_code_len = decode_varint(raw_hex[pos:])
    if height_code % 2 == 1:
        is_coinbase = True
        height_code -= 1
    else:
        is_coinbase = False
    height = height_code // 2

    # skip byte reserved only for backwards compatibility, should always be 0x00
    pos += height_code_len + 1

    # decode compressed txout amount
    compressed_amt, compressed_amt_len = decode_varint(raw_hex[pos:])
    amt = decompress_txout_amt(compressed_amt)
    pos += compressed_amt_len

    script_hex, script_pub_key_compressed_len = extract_from_hex(raw_hex[pos:])
    script_hex_len = len(script_hex)
    script_type, address = decompress_script(script_hex)
    output_len = pos + script_hex_len

    output = {
        'Address': address,
        'Script': script_type,
        'Amount': amt,
        # 'Height': height
    }
    return output, output_len

def extract_from_hex(raw_hex):
    if raw_hex[0] in (0x00, 0x01):
        return (raw_hex[:21], 21)
    elif raw_hex[0] in (0x02, 0x03):
        return (raw_hex[:33], 33)
    elif raw_hex[0] in (0x04, 0x05):
        return (raw_hex[:33], 33)
    else:
        script_len_code, script_len_code_len = decode_varint(raw_hex)
        real_script_len = script_len_code - NSPECIALSCRIPTS
        return (raw_hex[:script_len_code_len+real_script_len], real_script_len)

def get_block(blockfile, start_pos=0):
    pos = start_pos

    with open(blockfile, "rb") as f:
        # mmap.mmap is a way to access large files without loading them into memory as they were byte arrays
        raw_data = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        length = len(raw_data)
        magic_number = ''
        while magic_number != BITCOIN_CONSTANT:
            magic_number = raw_data[pos:pos+4]
            pos += 4
            if magic_number == None:
                return None                
            
        # size of the block (4 bytes)
        size = struct.unpack("<I", raw_data[pos:pos+4])[0]
        # go to the next block (skip both size of the block (4 bytes) and the actual size of it)
        pos += 4 + size
        return raw_data[pos-size:pos]