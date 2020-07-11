import cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import os

def getPrivateKey():
    currentPath = os. getcwd() + "\private_key.pem"
    if os.path.isfile(currentPath) == False:
        private_key = rsa.generate_private_key(
            public_exponent=123453,
            key_size=2048,
            backend=default_backend()
        )
        #public_key = private_key.public_key()
        storePrivateKey(private_key)
        return private_key
    else:
        print("A private key file was already found: " + currentPath + "\nIf you want to get a new key delete the private_key file from the directory and restart.")
        return

def getPublicKey(private_key):
    currentPath = os. getcwd() + "\public_key.pem"
    if os.path.isfile(currentPath) == False:
        public_key = private_key.public_key()
        storePublicKey(public_key)
        return public_key
    else:
        print("A public key file was already found: " + currentPath + "\nIf you want to get a new public key, delete the private_key file from the directory and restart.")
        return openPublicKey()

def storePrivateKey(private_key):
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    #print(pem)
    with open('private_key.pem', 'wb') as f:
        f.write(pem)

def storePublicKey(public_key):
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    #print(pem)
    with open('public_key.pem', 'wb') as f:
        f.write(pem)

def openPrivateKey():
    with open("private_key.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

def openPublicKey():
    with open("public_key.pem", "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    return public_key

def encrypt(public_key):
    print("Enter your message (180 characters max):")
    message = str(input())
    if len(message) > 0 and len(message) < 180:
        #print("Your message:" + message)
        message = message.encode('utf-8')
        encryptedTxT = public_key.encrypt(
            message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        print("Your encrypted message:" + str(encryptedTxT))
        return encryptedTxT
    elif len(message) == 0:
        print("The message is empty!")
        return
    elif len(message) > 180:
        print("The message is to long!")
        return

def decrypt(private_key, encryptedTxT):
    original_message = private_key.decrypt(
        encryptedTxT,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    print("The decrypted message is:")
    return print(original_message)

'''
#testing Key generation
priv = getPrivateKey()
pub = getPublicKey(priv)
print("\nPrivate Key:")
print(getPrivateKey())
#storePrivateKey(priv)
print("\nPublic:")
getPublicKey(priv)
#storePublicKey(pub)

decrypt(priv, (encrypt(pub)))

decrypt(priv,b'\x1a\x0ePw<\xd6-\x80\xe6\x93d\xe0\x99\\\x1a!\xae\x9c\xa8mr\xc9\xac{f\xf8_"\xc5\xb7\xe5\x80a\xeb\x11n\xc7\xc6\x91\xb1f7\x03\xd3:\xad\x7fh\x803\xb7\xf2\x93p\xe3\xaf9\x1c\xe3T>.\x04\xb4\xc1\x81\xab\x12\x83\r\x92\x0e:\xea\x83\xcc(d\xed,\x87\xa7\xc0\xe6\x9d9\xa9\x80C\xf1\xb0\xbf\xf8\x10\x1bK\x9f\xc5\x18z\x92V\x88\xa8:\x82M\xde`\x02Y\x85\x06\xe0d\xd8\xc0TD\xdb\xfb\x16\x82+\xf9F\x05iC\xaa\x8c\x10\xd0P\xe8\xa4\x96\x98#rK\xb7@\xee\xb8\x89\xb7u\x12`\x18\xf4\n\x0bE!uA\xb7%\x0b\xf1k\xf3\x8c\xe8D\xd2u\x1be@\xf5y\xbe\x19\x94u\xee\x98\'\x0fH\xc4\xf7\xc6\xd5e\x0b\xa4m\x7f\xd1~2\xbc+\'\xe4\x7f\xac\xaa\xee\x92\xef@\x882\xe6\xfd\xdfa\x9f5\x11H\xbc\xbf\x17\xf1O(\xddH\xa9\xb6\x06S\x08C\xcd\x16|T\xffg\x86\xec=\x98\x9c\xc4\xf1i\x19\xebm\xfa\xc0BG\x06\xb3FT?')

'''