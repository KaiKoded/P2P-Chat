from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import os

# Stark abgewandelt aber inspiriert von https://nitratine.net/blog/post/asymmetric-encryption-and-decryption-in-python/

def getPrivateKey():
    currentPath = os.getcwd() + "\private_key.pem"
    if not os.path.isfile(currentPath):
        print("Neuer privater Schlüssel wird generiert.")
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        storePrivateKey(private_key)
        return private_key
    else:
        print("Existierender privater Schlüssel gefunden.")
        return readPrivateKey()


def getPublicKey(private_key):
    currentPath = os.getcwd() + "\public_key.pem"
    if not os.path.isfile(currentPath):
        print("Neuer öffentlicher Schlüssel wird generiert.")
        public_key = private_key.public_key()
        storePublicKey(public_key)
        return public_key
    else:
        print("Existierender öffentlicher Schlüssel gefunden.")
        return readPublicKey()


def storePrivateKey(private_key):
    print("Neu generierter privater Schlüssel wird gespeichert.")
    pem = serializePrivateKey(private_key)
    with open('private_key.pem', 'wb') as f:
        f.write(pem)


def storePublicKey(public_key):
    print("Neu generierter öffentlicher Schlüssel wird gespeichert.")
    pem = serializePublicKey(public_key)
    with open('public_key.pem', 'wb') as f:
        f.write(pem)


def readPrivateKey():
    with open("private_key.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key


def readPublicKey():
    with open("public_key.pem", "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend())
    return public_key


def encrypt(public_key, message):
    encrypted_message = public_key.encrypt(
        message,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return encrypted_message


def decrypt(private_key, encrypted_message):
    decrypted_message = private_key.decrypt(
        encrypted_message,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return decrypted_message


def serializePrivateKey(private_key):
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return pem


def unserializePrivateKey(serialized_key):
    private_key = serialization.load_pem_private_key(
        serialized_key,
        password=None,
        backend=default_backend())
    return private_key


def serializePublicKey(public_key):
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo)
    return pem


def unserializePublicKey(serialized_key):
    public_key = serialization.load_pem_public_key(
        serialized_key,
        backend=default_backend())
    return public_key


# TEST
# message_to_encrypt = os.urandom(16)
# print(message_to_encrypt)
# privkey = getPrivateKey()
# pubkey = getPublicKey(privkey)
# ser_pubkey = str(serializePublicKey(pubkey), "utf-8")
# print(ser_pubkey)
# unser_pubkey = unserializePublicKey(bytes(ser_pubkey, "utf-8"))
# print(unser_pubkey)
# encr_msg = encrypt(unser_pubkey, message_to_encrypt)
# print(message_to_encrypt)
# decr_msg = decrypt(privkey, encr_msg)
# print(decr_msg)
# print(message_to_encrypt == decr_msg)
