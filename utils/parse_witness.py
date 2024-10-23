import hashlib

def public_key_to_hash160(public_key_hex):
    # Step 1: Convert the public key from hex to bytes
    public_key_bytes = bytes.fromhex(public_key_hex)
    
    # Step 2: Perform SHA-256 hashing on the public key
    sha256_hash = hashlib.sha256(public_key_bytes).digest()
    
    # Step 3: Perform RIPEMD-160 hashing on the result of the SHA-256 hash
    ripemd160 = hashlib.new('ripemd160')
    ripemd160.update(sha256_hash)
    hash160 = ripemd160.digest()
    
    # Step 4: Convert the RIPEMD-160 hash to a hexadecimal string
    return hash160.hex()