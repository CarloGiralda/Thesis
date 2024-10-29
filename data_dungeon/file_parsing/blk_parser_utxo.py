import hashlib
import mmap
from utils.utils import reverse
from utils.parse_transaction_output import parse_transaction_output
from utils.parse_transaction_input import parse_transaction_input
from utils.address_conversion import input_script_to_addr

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
    transaction = {}

    # Version Number
    tmpHex = read_bytes_from_str(mmap_obj, pos, 4)
    pos += 4
    RawTX = reverse(tmpHex)
    tmpHex = ''

    # Check if there is the optional two-byte array flag for witness data
    Witness = False
    b = read_bytes_from_str(mmap_obj, pos, 1)
    tmpB = b.upper()
    bInt = int(b, 16)
    pos += 1
    
    if bInt == 0:
        pos += 1  # Skip the witness flag
        c = 0
        c = read_bytes_from_str(mmap_obj, pos, 1)
        bInt = int(c, 16)
        tmpB = c.upper()
        pos += 1
        Witness = True

    # Inputs count
    c = 0
    if bInt < 253:
        c = 1
        tmpHex = hex(bInt)[2:].upper().zfill(2)
        tmpB = ''
    if bInt == 253: c = 3
    elif bInt == 254: c = 5
    elif bInt == 255: c = 9
    for j in range(1, c):
        b = read_bytes_from_str(mmap_obj, pos, 1).upper()
        tmpHex = b + tmpHex
        pos += 1

    inCount = int(tmpHex, 16)

    tmpHex += tmpB
    RawTX += reverse(tmpHex)

    inputs = []

    # Inputs
    for m in range(inCount):
        input = {}

        tmpHex = read_bytes_from_str(mmap_obj, pos, 32)  # Previous Transaction Hash
        transaction['Previous Transaction Hash'] = tmpHex
        pos += 32
        RawTX += reverse(tmpHex)

        tmpHex = read_bytes_from_str(mmap_obj, pos, 4)  # Previous Transaction Output Index
        transaction['Previous Transaction Output Index'] = tmpHex
        pos += 4
        RawTX += reverse(tmpHex)

        b = read_bytes_from_str(mmap_obj, pos, 1)
        bInt = int(b, 16)
        tmpB = ''
        if bInt >= 253:
            tmpB = b.upper()
        tmpHex, offset = read_varint_from_str(mmap_obj, pos)  # Input script length
        scriptLength = int(tmpHex, 16)
        pos += offset
        tmpHex += tmpB
        RawTX += reverse(tmpHex)

        tmpHex = read_bytes_from_str(mmap_obj, pos, scriptLength, 'B')  # Input script
        pos += scriptLength
        script_type, public_key = parse_transaction_input(tmpHex)
        addr = input_script_to_addr(script_type, public_key)
        if isinstance(addr, bytes):
            addr = addr.decode('utf-8')
        input['Sender'] = addr
        input['Value'] = 0
        RawTX += tmpHex

        tmpHex = read_bytes_from_str(mmap_obj, pos, 4, 'B')  # Sequence number
        pos += 4
        RawTX += tmpHex

        inputs.append(input)

    transaction['Inputs'] = inputs

    # Outputs count
    b = read_bytes_from_str(mmap_obj, pos, 1)
    bInt = int(b, 16)
    tmpB = ''
    if bInt >= 253:
        tmpB = b.upper()
    tmpHex, offset = read_varint_from_str(mmap_obj, pos)
    outputCount = int(tmpHex, 16)
    tmpHex = tmpHex + tmpB
    pos += offset
    RawTX += reverse(tmpHex)

    outputs = []

    # Outputs
    for m in range(outputCount):
        output = {}

        tmpHex = read_bytes_from_str(mmap_obj, pos, 8)  # Value
        value = int(tmpHex, 16)
        pos += 8
        RawTX += reverse(tmpHex)

        b = read_bytes_from_str(mmap_obj, pos, 1)
        bInt = int(b, 16)
        tmpB = ''
        if bInt >= 253:
            tmpB = b.upper()
        tmpHex, offset = read_varint_from_str(mmap_obj, pos)  # Output script length
        scriptLength = int(tmpHex, 16)
        tmpHex = tmpHex + tmpB
        pos += offset
        RawTX += reverse(tmpHex)

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
        RawTX += tmpHex

        outputs.append(output)

    transaction['Outputs'] = outputs

    if Witness:
        for m in range(inCount):
            tmpHex, offset = read_varint_from_str(mmap_obj, pos)
            WitnessLength = int(tmpHex, 16)
            pos += offset
            for j in range(WitnessLength):
                tmpHex, offset = read_varint_from_str(mmap_obj, pos)
                WitnessItemLength = int(tmpHex, 16)
                pos += offset
                tmpHex = read_bytes_from_str(mmap_obj, pos, WitnessItemLength)
                pos += WitnessItemLength
    
    Witness = False

    # Locktime
    tmpHex = read_bytes_from_str(mmap_obj, pos, 4)
    pos += 4
    RawTX += reverse(tmpHex)

    # Transaction Hash
    tmpHex = RawTX
    tmpHex = bytes.fromhex(tmpHex)
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = tmpHex[::-1]
    tmpHex = tmpHex.hex().upper()
    transaction['Transaction Hash'] = tmpHex

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

    pos += 4 + 4 + 32 + 32 + 4 + 4 + 4  # Skip block size, version number, previous block hash, merkle root, timestamp, bits, nonce

    # Transaction count
    tx_count_bytes, offset = read_varint_from_str(mmap_obj, pos)
    tx_count = int(tx_count_bytes, 16)
    pos += offset

    transactions = []

    for index in range(tx_count):
        transaction, new_pos = parse_transaction(mmap_obj, pos)
        pos = new_pos

        transactions.append(transaction)

    block['Transactions'] = transactions

    return block

def block_parsing_utxo(file_path, start_pos=0):
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        return parse_block(mm, start_pos)