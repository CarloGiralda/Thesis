import hashlib
from binascii import unhexlify, hexlify
from utils.utils import read_bytes, read_varint, reverse, merkle_root, p2wpkh_to_address, sha256
from utils.parse_transaction_output import parse_transaction_output
from utils.parse_transaction_input import parse_transaction_input
from utils.parse_witness import public_key_to_hash160
from utils.address_conversion import input_script_to_addr

COINBASE_REWARD = 312500000 # expressed in satoshis
IS_COINBASE = False

def parse_transaction(f):
    global IS_COINBASE
    global COINBASE_REWARD
    transaction = {}

    # Version Number
    tmpHex = read_bytes(f,4)
    transaction['Version Number'] = int(tmpHex, 16)
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
    transaction['Inputs Count'] = tmpHex

    tmpHex = tmpHex + tmpB
    RawTX = RawTX + reverse(tmpHex)

    # Inputs
    inputs = []
    for m in range(inCount):
        input = {}

        # Previous Transaction Hash
        tmpHex = read_bytes(f,32)
        input['Previous Transaction Hash'] = tmpHex
        if tmpHex == '0000000000000000000000000000000000000000000000000000000000000000':
            IS_COINBASE = True
        RawTX = RawTX + reverse(tmpHex)

        # Previous Transaction Output Index
        tmpHex = read_bytes(f,4)                
        input['Previous Transaction Output Index'] = tmpHex
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
        input['Script Type'] = script_type
        input['Sender'] = addr
        #input['Input Script'] = tmpHex
        RawTX = RawTX + tmpHex
        # Sequence number
        tmpHex = read_bytes(f,4,'B')
        input['Sequence Number'] = int(tmpHex, 16)
        RawTX = RawTX + tmpHex

        inputs.append(input)
    
    transaction['Inputs'] = inputs

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
    tmpHex = tmpHex + tmpB
    transaction['Outputs Count'] = str(outputCount)
    RawTX = RawTX + reverse(tmpHex)

    reward = 0

    # Outputs
    outputs = []
    for m in range(outputCount):
        output = {}

        # Value
        tmpHex = read_bytes(f,8)
        value = int(tmpHex, 16)
        output['Value'] = value
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
        output['Script Type'] = script_type
        output['Receiver'] = address
        RawTX = RawTX + tmpHex

        outputs.append(output)
    
    transaction['Outputs'] = outputs

    if IS_COINBASE:
        fees = reward - COINBASE_REWARD
        transaction['Reward'] = reward
        transaction['Fees'] = fees
        IS_COINBASE = False

    # Witnesses
    if Witness == True:
        witnesses = []
        for m in range(inCount):
            witness = {}
            tmpHex = read_varint(f)
            WitnessLength = int(tmpHex, 16)
            witness['Stack Items'] = WitnessLength
            for j in range(WitnessLength):
                tmpHex = read_varint(f)
                WitnessItemLength = int(tmpHex, 16)
                tmpHex = read_bytes(f, WitnessItemLength)
                witness[j] = {
                    'size': WitnessItemLength,
                    'item': tmpHex
                }

                if WitnessItemLength == 33: # it means that the item is a public key
                    public_key = reverse(tmpHex)
                    public_key_hash = public_key_to_hash160(public_key)
                    address = p2wpkh_to_address(bytes.fromhex(public_key_hash))
                    if transaction['Inputs'][m]['Script Type'] == "SegWit Input" and transaction['Inputs'][m]['Sender'] == None:
                        transaction['Inputs'][m]['Sender'] = address

            witnesses.append(witness)
    
        transaction['Witnesses'] = witnesses

    Witness = False
    # Locktime
    tmpHex = read_bytes(f,4)
    transaction['Locktime'] = tmpHex
    RawTX = RawTX + reverse(tmpHex)
    # Transaction Hash
    tmpHex = RawTX
    tmpHex = bytes.fromhex(tmpHex)
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = hashlib.new('sha256', tmpHex).digest()
    tmpHex = tmpHex[::-1]
    tmpHex = tmpHex.hex().upper()
    transaction['Transaction Hash'] = tmpHex

    return transaction

def parse_block(f):
    block = {}

    tmpHex = ''

    # cycle until you reach the magic number (value at the beginning of each block)
    # it is for to skip zeroes in some blk files
    while tmpHex != 'D9B4BEF9':
        # Magic Number
        tmpHex = read_bytes(f,4)
        block['Magic Number'] = tmpHex

    # Block size
    tmpHex = read_bytes(f,4)
    block['Block Size'] = int(tmpHex, 16)

    # Block Header
    pos = f.tell()
    tmpHex = read_bytes(f, 80, 'B')
    block_header = tmpHex
    f.seek(pos)

    # Block Header
    # - Version Number
    tmpHex = read_bytes(f,4)
    block['Version Number'] = int(tmpHex, 16)
    # - Hash of previous block
    tmpHex = read_bytes(f,32)
    block['Previous Block Hash'] = tmpHex
    # - Merkle Root
    tmpHex = read_bytes(f,32)
    block['Merkle Root'] = tmpHex
    MerkleRoot = tmpHex
    # - Timestamp
    tmpHex = read_bytes(f,4)
    block['Timestamp'] = int(tmpHex, 16)
    # - Target Difficulty
    tmpHex = read_bytes(f,4)
    block['Bits'] = int(tmpHex, 16)
    # - Nonce
    tmpHex = read_bytes(f,4)
    block['Nonce'] = int(tmpHex, 16)

    # Block Hash
    block_header_bytes = unhexlify(block_header)
    hash = sha256(sha256(block_header_bytes))
    block_hash = hexlify(hash[::-1]).decode('utf-8')
    block['Block Hash'] = block_hash

    # Transaction count
    tmpHex = read_varint(f)
    txCount = int(tmpHex, 16)
    block['Transactions Count'] = str(txCount)

    tmpHex = ''
    tx_hashes = []

    # Transactions
    transactions = []
    for k in range(txCount):
        transaction = parse_transaction(f)
        tx_hashes.append(transaction['Transaction Hash'])

        transactions.append(transaction)

    # Check if Merkle Root is correct
    tx_hashes = [bytes.fromhex(h) for h in tx_hashes]
    tmpHex = merkle_root(tx_hashes).hex().upper()
    if tmpHex != MerkleRoot:
        print ('Merkle roots does not match! >', MerkleRoot, tmpHex)
    
    block['Transactions'] = transactions

    return block

def block_parsing(f):
    return parse_block(f)