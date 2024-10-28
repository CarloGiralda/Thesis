import struct

BLOCK_HAVE_DATA = 8
BLOCK_HAVE_UNDO = 16

def _read_varint(raw_hex):
    """
    Reads the weird format of VarInt present in src/serialize.h of bitcoin core
    and being used for storing data in the leveldb.
    This is not the VARINT format described for general bitcoin serialization
    use.
    """
    n = 0
    pos = 0
    while True:
        data = raw_hex[pos]
        pos += 1
        n = (n << 7) | (data & 0x7f)
        if data & 0x80 == 0:
            return n, pos
        n += 1

def format_hash(hash_):
    return hash_[::-1].hex()

def block_index(blk_hash, raw_hex):
    hash = blk_hash
    pos = 0
    n_version, i = _read_varint(raw_hex[pos:])
    pos += i
    height, i = _read_varint(raw_hex[pos:])
    pos += i
    status, i = _read_varint(raw_hex[pos:])
    pos += i
    n_tx, i = _read_varint(raw_hex[pos:])
    pos += i
    if status & (BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO):
        file, i = _read_varint(raw_hex[pos:])
        pos += i
    else:
        file = -1

    if status & BLOCK_HAVE_DATA:
        data_pos, i = _read_varint(raw_hex[pos:])
        pos += i
    else:
        data_pos = -1
    if status & BLOCK_HAVE_UNDO:
        undo_pos, i = _read_varint(raw_hex[pos:])
        pos += i
    else:
        undo_pos = -1

    assert (pos + 80 == len(raw_hex))
    version, p, m, time, bits, nonce = struct.unpack(
        "<I32s32sIII",
        raw_hex[-80:]
    )
    prev_hash = format_hash(p)
    merkle_root = format_hash(m)

    return (height, file, data_pos, undo_pos)