import mmap
from binascii import unhexlify, hexlify
from utils.utils import reverse, p2wpkh_to_address, sha256
from utils.parse_transaction_output import parse_transaction_output
from utils.parse_transaction_input import parse_transaction_input
from utils.parse_witness import public_key_to_hash160
from utils.address_conversion import input_script_to_addr

COINBASE_REWARD = 312500000 # expressed in satoshis
IS_COINBASE = False

def read_bytes_from_str(str, pos, n, byte_order = 'L'):
    data = str[pos:pos+n]
    if len(data) < n:
        return None
    if byte_order == 'L':
        data = data[::-1]
    data = data.hex().upper()
    return data

def read_varint_from_str(str, pos):
    b = str[pos:pos+1]
    bInt = int(b.hex(), 16)
    c = 0
    data = ''

    if bInt < 253:
        c = 1
        data = b.hex().upper()
    if bInt == 253: c = 3
    if bInt == 254: c = 5
    if bInt == 255: c = 9

    new_pos = pos + 1
    for _ in range(1,c):
        b = str[new_pos:new_pos+1]
        b = b.hex().upper()
        data = b + data
        new_pos += 1

    return data, new_pos - pos

def parse_transaction(mmap_obj, pos):
    global IS_COINBASE
    global COINBASE_REWARD
    transaction = {}

    tmpHex = ''

    # Version Number
    pos += 4

    # Check if there is the optional two-byte array flag for witness data
    Witness = False
    b = read_bytes_from_str(mmap_obj, pos, 1)
    bInt = int(b, 16)
    pos += 1
    
    if bInt == 0:
        pos += 1  # Skip the witness flag
        c = 0
        c = read_bytes_from_str(mmap_obj, pos, 1)
        bInt = int(c, 16)
        pos += 1
        Witness = True

    # Inputs count
    c = 0
    if bInt < 253:
        c = 1
        tmpHex = hex(bInt)[2:].upper().zfill(2)
    if bInt == 253: c = 3
    elif bInt == 254: c = 5
    elif bInt == 255: c = 9
    for j in range(1, c):
        b = read_bytes_from_str(mmap_obj, pos, 1).upper()
        tmpHex = b + tmpHex
        pos += 1

    inCount = int(tmpHex, 16)

    inputs = []

    # Inputs
    for m in range(inCount):
        input = {}

        tmpHex = read_bytes_from_str(mmap_obj, pos, 32)  # Previous Transaction Hash
        pos += 32
        if tmpHex == '00' * 32:
            IS_COINBASE = True

        # Previous Transaction Output Index
        pos += 4

        tmpHex, offset = read_varint_from_str(mmap_obj, pos)  # Input script length
        scriptLength = int(tmpHex, 16)
        pos += offset

        tmpHex = read_bytes_from_str(mmap_obj, pos, scriptLength, 'B')  # Input script
        pos += scriptLength
        script_type, public_key = parse_transaction_input(tmpHex)
        addr = input_script_to_addr(script_type, public_key)
        input['Sender'] = addr
        input['Value'] = 0

        # Sequence number
        pos += 4

        inputs.append(input)

    transaction['Inputs'] = inputs

    # Outputs count
    tmpHex, offset = read_varint_from_str(mmap_obj, pos)
    outputCount = int(tmpHex, 16)
    pos += offset

    reward = 0
    outputs = []

    # Outputs
    for m in range(outputCount):
        output = {}

        tmpHex = read_bytes_from_str(mmap_obj, pos, 8)  # Value
        value = int(tmpHex, 16)
        pos += 8
        if IS_COINBASE:
            reward += value

        tmpHex, offset = read_varint_from_str(mmap_obj, pos)  # Output script length
        scriptLength = int(tmpHex, 16)
        pos += offset

        tmpHex = read_bytes_from_str(mmap_obj, pos, scriptLength, 'B')  # Output script
        script_type, address = parse_transaction_output(tmpHex)
        if isinstance(address, bytes):
            address = address.decode('utf-8')
        elif isinstance(address, list):
            address = 'UNKNOWN'
        # if the first two elements of the address are 6a (= OP_RETURN), then it means that the output is invalid
        if address[:2] == '6a':
            address = 'INVALID'
        pos += scriptLength
        output['Receiver'] = address
        output['Value'] = value

        outputs.append(output)

    transaction['Outputs'] = outputs

    if IS_COINBASE:
        fees = reward - COINBASE_REWARD
        transaction['Reward'] = reward
        transaction['Fees'] = fees
        IS_COINBASE = False

    if Witness:
        for m in range(inCount):
            tmpHex, offset = read_varint_from_str(mmap_obj, pos)
            WitnessLength = int(tmpHex, 16)
            pos += offset

            witnesses = []

            for j in range(WitnessLength):
                tmpHex, offset = read_varint_from_str(mmap_obj, pos)
                WitnessItemLength = int(tmpHex, 16)
                pos += offset
                tmpHex = read_bytes_from_str(mmap_obj, pos, WitnessItemLength)
                pos += WitnessItemLength

                # save both the length and the value
                witnesses.append(WitnessItemLength)
                witnesses.append(tmpHex)
            
            if len(witnesses) == 4:
                # addresses can be retrieved by witnesses if the transaction is either P2WPKH or P2SH-P2WPKH
                if witnesses[0] == 72 and witnesses[2] == 33 and transaction['Inputs'][m]['Sender'] == None:
                    public_key = reverse(witnesses[3])
                    public_key_hash = public_key_to_hash160(public_key)
                    address = p2wpkh_to_address(bytes.fromhex(public_key_hash))

                    transaction['Inputs'][m]['Sender'] = address
    
    Witness = False

    # Locktime
    pos += 4

    return transaction, pos

def parse_block(mmap_obj, start_pos=0):
    """Parses a block starting from the specified position."""
    block = {}
    pos = start_pos
    tmpHex = ''

    while tmpHex != 'D9B4BEF9':  # Magic number
        tmpHex = read_bytes_from_str(mmap_obj, pos, 4)
        pos += 4
        if tmpHex == None:
            return None

    # Block Size
    pos += 4

    # Block Header
    block_header = read_bytes_from_str(mmap_obj, pos, 80, 'B')
    block_header_bytes = unhexlify(block_header)
    hash = sha256(sha256(block_header_bytes))
    block_hash = hexlify(hash[::-1]).decode('utf-8').upper()

    pos += 4 # Skip version number
    previous_block_hash = read_bytes_from_str(mmap_obj, pos, 32)
    pos += 32
    pos += 32 + 4 + 4 + 4  # Skip merkle root, timestamp, bits, nonce

    # Transaction count
    tx_count_bytes, offset = read_varint_from_str(mmap_obj, pos)
    tx_count = int(tx_count_bytes, 16)
    pos += offset

    block['Block Hash'] = block_hash
    block['Previous Block Hash'] = previous_block_hash
    transactions = []

    for index in range(tx_count):
        transaction, new_pos = parse_transaction(mmap_obj, pos)
        pos = new_pos

        # if it is 0, then the transaction is a coinbase transaction
        if index == 0:
            reward = transaction.pop('Reward')
            fees = transaction.pop('Fees')
            block['Reward'] = reward
            block['Fees'] = fees

        transactions.append(transaction)

    block['Transactions'] = transactions

    return block

def block_parsing(file_path, start_pos=0):
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        return parse_block(mm, start_pos)