from utils.utils import p2pkh_to_address, p2sh_to_address, p2wpkh_to_address, p2wsh_to_address, p2tr_to_address

# Script parsers
def parse_p2pkh(script):
    if len(script) == 25 and script[:3] == b'\x76\xa9\x14' and script[-2:] == b'\x88\xac':
        pubkey_hash = script[3:-2]
        return 'P2PKH', p2pkh_to_address(pubkey_hash)
    return None, None

def parse_p2sh(script):
    if len(script) == 23 and script[:2] == b'\xa9\x14' and script[-1:] == b'\x87':
        script_hash = script[2:-1]
        return 'P2SH', p2sh_to_address(script_hash)
    return None, None

def parse_p2wpkh(script):
    if len(script) == 22 and script[:2] == b'\x00\x14':
        pubkey_hash = script[2:]
        return 'P2WPKH', p2wpkh_to_address(pubkey_hash)
    return None, None

def parse_p2wsh(script):
    if len(script) == 34 and script[:2] == b'\x00\x20':
        script_hash = script[2:]
        return 'P2WSH', p2wsh_to_address(script_hash)
    return None, None

def parse_p2tr(script):
    if len(script) == 34 and script[:2] == b'\x51\x20':
        script_hash = script[2:]
        return 'P2TR', p2tr_to_address(script_hash)
    return None, None

def parse_multisig(script):
    # Multisig scripts start with OP_M (0x52 - 0x60), have n public keys, and end with OP_N (0x52 - 0x60) OP_CHECKMULTISIG
    if len(script) >= 3 and 0x51 <= script[0] <= 0x60 and script[-1] == 0xae:
        m = script[0] - 0x50
        n = script[-2] - 0x50
        pubkeys = []
        idx = 1
        while len(pubkeys) < n and idx < len(script) - 2:
            pubkey_len = script[idx]
            idx += 1
            pubkeys.append(script[idx:idx + pubkey_len].hex())
            idx += pubkey_len
        return f'Multisig {m}-of-{n}', pubkeys
    return None, None

def parse_script(script):
    parsers = [parse_p2pkh, parse_p2sh, parse_p2wpkh, parse_p2wsh, parse_p2tr, parse_multisig]
    for parser in parsers:
        script_type, result = parser(script)
        if script_type:
            return script_type, result
    return 'Unknown', script.hex()

def parse_transaction_output(script_hex):
    script = bytes.fromhex(script_hex)
    script_type, address_or_pubkeys = parse_script(script)
    if isinstance(address_or_pubkeys, bytes):
        address_or_pubkeys = address_or_pubkeys.decode('utf-8')
    return script_type, address_or_pubkeys