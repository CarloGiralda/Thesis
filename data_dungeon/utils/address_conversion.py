import base58
from utils.utils import sha256, ripemd160

def public_key_to_address(hex_string, legacy=True):
    # 1. Perform SHA256 on the input string (convert hex string to bytes first)
    data = bytes.fromhex(hex_string)
    sha256_result = sha256(data)

    # 2. Perform RIPEMD-160 on the result of SHA256
    ripemd160_result = ripemd160(sha256_result)

    if legacy:
        # 3. Add '00' byte at the beginning (for mainnet Bitcoin address)
        extended_ripemd160 = b'\x00' + ripemd160_result
    else:
        # 3. Add '05' byte at the beginning (for mainnet Bitcoin address)
        extended_ripemd160 = b'\x05' + ripemd160_result

    # 4. Perform SHA256 on the result of step 3
    sha256_step4 = sha256(extended_ripemd160)

    # 5. Perform SHA256 again on the result of step 4
    sha256_step5 = sha256(sha256_step4)

    # 6. Take the first 4 bytes of the result of step 5 and append them to step 3
    checksum = sha256_step5[:4]
    final_result = extended_ripemd160 + checksum

    # 7. Perform Base58 encoding on the final result
    base58_result = base58.b58encode(final_result)

    return base58_result.decode('utf-8')

def input_script_to_addr(script_type, public_key):
    if script_type == "P2PKH Input":
        # addresses start with 1
        return public_key_to_address(public_key, True)
    elif script_type == "P2SH-P2WPKH Input":
        # addresses start with 3
        return public_key_to_address('0014' + public_key, False)
    else:
        return None