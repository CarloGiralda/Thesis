import hashlib
from binascii import unhexlify, hexlify
from utils.utils import read_bytes, read_varint, reverse, p2wpkh_to_address, sha256
from utils.parse_transaction_output import parse_transaction_output
from utils.parse_transaction_input import parse_transaction_input
from utils.parse_witness import public_key_to_hash160
from utils.address_conversion import input_script_to_addr

COINBASE_REWARD = 312500000 # expressed in satoshis
IS_COINBASE = False

def parse_transaction(f):
    global IS_COINBASE
    global COINBASE_REWARD
    transaction = []

    # Version Number
    tmpHex = read_bytes(f,4)
    RawTX = reverse(tmpHex)
    tmpHex = ''

    # Check if there is the optional two byte array flag for witness data
    Witness = False
    b = f.read(1)
    tmpB = b.hex().upper()
    bInt = int(b.hex(), 16)
    # flag = 0001 (two bytes)
    if bInt == 0:
        tmpB = ''
        # seek(offset, whence)
        # - offset: number of positions of the pointer to move within the file
        # - whence: 0 -> absolute file positioning, 1 -> relative to the current position, 2 -> relative to the end of the file
        f.seek(1, 1) # this skips the whole flag
        c = 0
        c = f.read(1)
        bInt = int(c.hex(), 16)
        tmpB = c.hex().upper()
        Witness = True

    # Inputs count
    c = 0
    if bInt < 253:
        c = 1
        # int -> hex string -> removes initial '0x' -> turns all into capital -> pads until string has at least two characters
        tmpHex = hex(bInt)[2:].upper().zfill(2)
        tmpB = ''
    if bInt == 253: c = 3
    if bInt == 254: c = 5
    if bInt == 255: c = 9
    for j in range(1,c):
        b = f.read(1)
        b = b.hex().upper()
        tmpHex = b + tmpHex
    inCount = int(tmpHex,16)
    transaction.append(f'Inputs Count: {inCount}')

    tmpHex = tmpHex + tmpB
    RawTX = RawTX + reverse(tmpHex)

    # Inputs
    for m in range(inCount):

        # Previous Transaction Hash
        tmpHex = read_bytes(f,32)
        transaction.append(f'Previous Transaction Hash: {tmpHex}')
        if tmpHex == '0000000000000000000000000000000000000000000000000000000000000000':
            IS_COINBASE = True
        RawTX = RawTX + reverse(tmpHex)

        # Previous Transaction Output Index
        tmpHex = read_bytes(f,4)
        transaction.append(f'Previous Transaction Output Index: {tmpHex}')
        RawTX = RawTX + reverse(tmpHex)

        # save value for later and restore the previous pointer position
        pos = f.tell()
        b = f.read(1)
        bInt = int(b.hex(), 16)
        tmpB = ''
        if (bInt >= 253):
            tmpB = b.hex().upper()
        f.seek(pos)
        # Varint
        tmpHex = read_varint(f)
        # Input script
        scriptLength = int(tmpHex,16)
        tmpHex = tmpHex + tmpB
        RawTX = RawTX + reverse(tmpHex)
        tmpHex = read_bytes(f,scriptLength,'B')
        script_type, public_key = parse_transaction_input(tmpHex)
        addr = input_script_to_addr(script_type, public_key)
        transaction.append(f'Sender: {addr}')
        RawTX = RawTX + tmpHex

        # Sequence number
        tmpHex = read_bytes(f,4,'B')
        transaction.append(f'Sequence Number: {int(tmpHex, 16)}')
        RawTX = RawTX + tmpHex

    # save value for later and restore the previous pointer position
    pos = f.tell()
    b = f.read(1)
    bInt = int(b.hex(), 16)
    tmpB = ''
    if (bInt >= 253):
        tmpB = b.hex().upper()
    f.seek(pos)
    # Varint
    tmpHex = read_varint(f)
    # Outputs counts
    outputCount = int(tmpHex,16)
    transaction.append(f'Outputs Count: {outputCount}')
    tmpHex = tmpHex + tmpB
    RawTX = RawTX + reverse(tmpHex)

    reward = 0

    # Outputs
    for m in range(outputCount):

        # Value
        tmpHex = read_bytes(f,8)
        value = int(tmpHex, 16)
        transaction.append(f'Value: {value}')
        if IS_COINBASE:
            reward += value
        RawTX = RawTX + reverse(tmpHex)

        # save value for later and restore the previous pointer position
        pos = f.tell()
        b = f.read(1)
        bInt = int(b.hex(), 16)
        tmpB = ''
        if (bInt >= 253):
            tmpB = b.hex().upper()
        f.seek(pos)
        # Varint
        tmpHex = read_varint(f)
        scriptLength = int(tmpHex, 16)
        tmpHex = tmpHex + tmpB
        RawTX = RawTX + reverse(tmpHex)
        # Output script
        tmpHex = read_bytes(f, scriptLength, 'B')
        script_type, address = parse_transaction_output(tmpHex)
        transaction.append(f'Receiver: {address}')
        RawTX = RawTX + tmpHex

    if IS_COINBASE:
        fees = reward - COINBASE_REWARD
        transaction.insert(0, f'Reward: {reward}')
        transaction.insert(1, f'Fees: {fees}')
        IS_COINBASE = False

    # Witnesses
    if Witness == True:
        for m in range(inCount):
            tmpHex = read_varint(f)
            WitnessLength = int(tmpHex, 16)
            for j in range(WitnessLength):
                tmpHex = read_varint(f)
                WitnessItemLength = int(tmpHex, 16)
                tmpHex = read_bytes(f, WitnessItemLength)

                if WitnessItemLength == 33: # it means that the item is a public key
                    public_key = reverse(tmpHex)
                    public_key_hash = public_key_to_hash160(public_key)
                    address = p2wpkh_to_address(bytes.fromhex(public_key_hash))

                    counter = -1
                    for i in range(1, len(transaction)):
                        if 'Previous Transaction Hash' in transaction[i]:
                            counter += 1
                            if m == counter and transaction[i + 2] == 'Sender: None':
                                # index + 2 corresponds to the Sender field
                                transaction[i + 2] = f'Sender: {address}'
                                break
                            # Wrapped Segwit inputs (compatibility layer)
                            elif m == counter and transaction[i + 2] != 'Sender: None':
                                break

    Witness = False
    # Locktime
    tmpHex = read_bytes(f,4)
    transaction.append(f'Locktime: {tmpHex}')
    RawTX = RawTX + reverse(tmpHex)
    # Transaction Hash
    tmpHex = RawTX
    tmpHex = bytes.fromhex(tmpHex)
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = tmpHex[::-1]
    tmpHex = tmpHex.hex().upper()
    transaction.append(f'Transaction Hash: {tmpHex}')

    return transaction

def parse_block(f):

    tmpHex = ''

    # cycle until you reach the magic number (value at the beginning of each block)
    # it is for to skip zeroes in some blk files
    while tmpHex != 'D9B4BEF9':
        # Magic Number
        tmpHex = read_bytes(f, 4)

    # Block size
    tmpHex = read_bytes(f, 4)

    # Block Header
    pos = f.tell()
    tmpHex = read_bytes(f, 80, 'B')
    block_header = tmpHex
    f.seek(pos)

    # Skip 1. Version Number
    f.seek(4, 1)
    # Hash of previous block
    tmpHex = read_bytes(f, 32)
    previous_block_hash = tmpHex
    # Skip 1. Merkle Root 2. Timestamp 3. Bits 4. Nonce
    f.seek(32 + 4 + 4 + 4, 1)

    block_header_bytes = unhexlify(block_header)
    hash = sha256(sha256(block_header_bytes))
    block_hash = hexlify(hash[::-1]).decode('utf-8')

    # Transaction count
    tmpHex = read_varint(f)
    txCount = int(tmpHex, 16)

    tx_hashes = []

    # Transactions
    transactions = []
    transactions.append([f'Block Hash: {block_hash}', f'Previous Block Hash: {previous_block_hash}'])
    for k in range(txCount):
        transaction = parse_transaction(f)
        tx_hashes.append(transaction[-1][17:])

        transactions.append(transaction)
    
    return transactions

def transaction_parsing(f):
    return parse_block(f)