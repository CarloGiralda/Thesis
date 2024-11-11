import hashlib
import base58
import struct
from enum import Enum
from binascii import hexlify

# -------------------------------------------------------
# Used in nlk*.dat file parsing
# -------------------------------------------------------

# reverse byte order
def reverse(input):
    L = len(input)
    if (L % 2) != 0:
        return None
    else:
        Res = ''
        L = L // 2
        for i in range(L):
            T = input[i*2] + input[i*2+1]
            Res = T + Res
            T = ''
        return (Res)

def merkle_root(lst):
    sha256d = lambda x: hashlib.sha256(hashlib.sha256(x).digest()).digest()
    hash_pair = lambda x, y: sha256d(x[::-1] + y[::-1])[::-1]
    if len(lst) == 1: return lst[0]
    if len(lst) % 2 == 1:
        lst.append(lst[-1])
    return merkle_root([hash_pair(x,y) for x, y in zip(*[iter(lst)]*2)])

def read_bytes(file,n,byte_order = 'L'):
    data = file.read(n)
    if len(data) < n:
        return None
    if byte_order == 'L':
        data = data[::-1]
    data = data.hex().upper()
    return data

# Dynamic representation of integers based on their length:
# - first byte < 253 ---> the integer is represented as a single byte (the first one)
# - first byte = 253 ---> the integer is represented as two bytes (the subsequent ones)
# - first byte = 254 ---> the integer is represented as four bytes (the subsequent ones)
# - first byte = 255 ---> the integer is represented as eight bytes (the subsequent ones)
def read_varint(file):
    b = file.read(1)
    bInt = int(b.hex(), 16)
    c = 0
    data = ''

    if bInt < 253:
        c = 1
        data = b.hex().upper()
    if bInt == 253: c = 3
    if bInt == 254: c = 5
    if bInt == 255: c = 9

    for _ in range(1,c):
        b = file.read(1)
        b = b.hex().upper()
        data = b + data

    return data

# -------------------------------------------------------
# Used in address conversion
# -------------------------------------------------------

class Encoding(Enum):
    """Enumeration type to list the various supported encodings."""
    BECH32 = 1
    BECH32M = 2

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
BECH32M_CONST = 0x2bc830a3

def base58_check_encode(prefix, payload):
    data = prefix + payload
    checksum = hashlib.sha256(hashlib.sha256(data).digest()).digest()[:4]
    return base58.b58encode(data + checksum)

def sha256(data):
    """Compute SHA256 hash."""
    return hashlib.sha256(data).digest()

def ripemd160(data):
    """Compute RIPEMD-160 hash."""
    h = hashlib.new('ripemd160')
    h.update(data)
    return h.digest()

def bech32_polymod(values):
    """Internal function that computes the Bech32 checksum."""
    generator = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for value in values:
        top = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ value
        for i in range(5):
            chk ^= generator[i] if ((top >> i) & 1) else 0
    return chk


def bech32_hrp_expand(hrp):
    """Expand the HRP into values for checksum computation."""
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]


def bech32_verify_checksum(hrp, data):
    """Verify a checksum given HRP and converted data characters."""
    const = bech32_polymod(bech32_hrp_expand(hrp) + data)
    if const == 1:
        return Encoding.BECH32
    if const == BECH32M_CONST:
        return Encoding.BECH32M
    return None

def bech32_create_checksum(hrp, data, spec):
    """Compute the checksum values given HRP and data."""
    values = bech32_hrp_expand(hrp) + data
    const = BECH32M_CONST if spec == Encoding.BECH32M else 1
    polymod = bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ const
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]


def bech32_encode(hrp, data, spec):
    """Compute a Bech32 string given HRP and data values."""
    combined = data + bech32_create_checksum(hrp, data, spec)
    return hrp + '1' + ''.join([CHARSET[d] for d in combined])

def bech32_decode(bech):
    """Validate a Bech32/Bech32m string, and determine HRP and data."""
    if ((any(ord(x) < 33 or ord(x) > 126 for x in bech)) or
            (bech.lower() != bech and bech.upper() != bech)):
        return (None, None, None)
    bech = bech.lower()
    pos = bech.rfind('1')
    if pos < 1 or pos + 7 > len(bech) or len(bech) > 90:
        return (None, None, None)
    if not all(x in CHARSET for x in bech[pos+1:]):
        return (None, None, None)
    hrp = bech[:pos]
    data = [CHARSET.find(x) for x in bech[pos+1:]]
    spec = bech32_verify_checksum(hrp, data)
    if spec is None:
        return (None, None, None)
    return (hrp, data[:-6], spec)

def convertbits(data, frombits, tobits, pad=True):
    """General power-of-2 base conversion."""
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret

# Address conversion based on script type
def p2pkh_to_address(pubkey_hash):
    return base58_check_encode(b'\x00', pubkey_hash)  # Mainnet P2PKH

def p2sh_to_address(script_hash):
    return base58_check_encode(b'\x05', script_hash)  # Mainnet P2SH

def p2wpkh_to_address(pubkey_hash):
    pubkey_hash_5_bit = convertbits(pubkey_hash, 8, 5, pad=False)
    return bech32_encode('bc', [0] + pubkey_hash_5_bit, spec=Encoding.BECH32) # Bech32 mainnet human-readable part (hrp)

def p2wsh_to_address(script_hash):
    script_hash_5_bit = convertbits(script_hash, 8, 5)
    return bech32_encode('bc', [0] + script_hash_5_bit, spec=Encoding.BECH32) # Bech32 mainnet human-readable part (hrp)

def p2tr_to_address(script_hash):
    script_hash_5_bit = convertbits(script_hash, 8, 5)
    return bech32_encode('bc', [1] + script_hash_5_bit, spec=Encoding.BECH32M) # Bech32 mainnet human-readable part (hrp)

# -------------------------------------------------------
# Used in rev*.dat file parsing
# -------------------------------------------------------

def decode_varint(raw_hex):
    """
    Reads the weird format of VarInt present in src/serialize.h of bitcoin core
    and being used for storing data in the leveldb.
    This is not the VARINT format described for general bitcoin serialization
    use.
    """
    n = 0
    pos = 0
    while True:
        try:
            data = raw_hex[pos]
        except IndexError as e:
            print("IndexError caught on raw_hex: ", raw_hex, e)
            raise e
        pos += 1
        n = (n << 7) | (data & 0x7f)
        if data & 0x80 == 0:
            return n, pos
        n += 1

def decode_compactsize(data):
    assert(len(data) > 0)
    size = int(data[0])
    assert(size <= 255)

    if size < 253:
        return size, 1

    if size == 253:
        format_ = '<H'
    elif size == 254:
        format_ = '<I'
    elif size == 255:
        format_ = '<Q'
    else:
        # Should never be reached
        assert 0, "unknown format_ for size : %s" % size

    size = struct.calcsize(format_)
    return struct.unpack(format_, data[1:size+1])[0], size + 1

def decompress_txout_amt(amount_compressed_int):
    # (this function stolen from https://github.com/sr-gi/bitcoin_tools and modified to remove bug)
    # No need to do any work if it's zero.
    if amount_compressed_int == 0:
        return 0

    # The decompressed amount is either of the following two equations:
    # x = 1 + 10*(9*n + d - 1) + e
    # x = 1 + 10*(n - 1)       + 9
    amount_compressed_int -= 1

    # The decompressed amount is now one of the following two equations:
    # x = 10*(9*n + d - 1) + e
    # x = 10*(n - 1)       + 9
    exponent = amount_compressed_int % 10
    
    # integer division
    amount_compressed_int //= 10

    # The decompressed amount is now one of the following two equations:
    # x = 9*n + d - 1  | where e < 9
    # x = n - 1        | where e = 9
    n = 0
    if exponent < 9:
        lastDigit = amount_compressed_int%9 + 1
        # integer division
        amount_compressed_int //= 9
        n = amount_compressed_int*10 + lastDigit
    else:
        n = amount_compressed_int + 1

    # Apply the exponent.
    return n * 10**exponent

def change_endianness(x):
    """ Changes the endianness (from BE to LE and vice versa) of a given value.

    :param x: Given value which endianness will be changed.
    :type x: hex str
    :return: The opposite endianness representation of the given value.
    :rtype: hex str
    """

    # If there is an odd number of elements, we make it even by adding a 0
    if (len(x) % 2) == 1:
        x += b'\x00'
    z = x[::-1]
    return hexlify(z)