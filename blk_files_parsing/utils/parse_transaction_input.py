import binascii

def parse_transaction_input(script_hex):
    """Parse and classify input script as P2PK, P2PKH, P2WPKH, P2WSH, or P2SH."""
    script_bytes = bytes.fromhex(script_hex)
    script_len = len(script_bytes)

    # P2PKH format: <signature> <public key>
    if script_len == 107 or script_len == 106:  # Typical lengths for P2PKH input scripts
        signature_len = script_bytes[0]
        pubkey_len = script_bytes[1 + signature_len]
        pubkey = script_bytes[1 + signature_len + 1: 1 + signature_len + 1 + pubkey_len]
        
        script_type = "P2PKH Input"
        public_key = f"{binascii.hexlify(pubkey).decode()}"
        return script_type, public_key

    # P2PK format: <signature> (No public key, public key was in previous output)
    elif script_len == 71 or script_len == 72:  # Typical lengths for P2PK input scripts        
        script_type = "P2PK Input"
        return script_type, None
    
    # P2WPKH or P2WSH format (segwit): <witness> (handled in witness section, not scriptSig)
    elif script_len == 0:  # Segwit input scripts typically have no scriptSig
        script_type = "SegWit Input"
        return script_type, None
    
    # P2SH format: <redeem script> (Could be a wrapped P2WPKH, P2WSH, etc.)
    elif script_len > 0:
        redeem_script_len = script_bytes[0]
        redeem_script = script_bytes[1:1 + redeem_script_len]
        script_type = "P2SH Input"
        public_key_hash = f"{binascii.hexlify(redeem_script).decode()}"
        
        # Check for nested P2WPKH (P2SH-P2WPKH)
        if len(redeem_script) == 22 and redeem_script[0] == 0x00 and redeem_script[1] == 0x14:
            pubkey_hash = redeem_script[2:22]
            script_type = "P2SH-P2WPKH Input"
            public_key_hash = f"{binascii.hexlify(pubkey_hash).decode()}"
        return script_type, public_key_hash
    
    # Unknown input script
    else:
        script_type = "Unknown Input"
        script = f"{script_hex}"
        return script_type, script