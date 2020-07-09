import cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

def getPrivateKey():
    private_key = rsa.generate_private_key(
        public_exponent=123453,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    return private_key

def getPublicKey(private_key):
    public_key = private_key.public_key()
    return public_key

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

def encrypt(message, public_key):
    if len(message) > 0:
        message = message.encode('utf-8')

        encryptedTxT = public_key.encrypt(
            message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return encryptedTxT
    else:
        pass

def decrypt(private_key, encryptedTxT):
    original_message = private_key.decrypt(
        encryptedTxT,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    print(original_message)
    return original_message


# Test ob gespeicherte Keys noch gleich sind
priv = getPrivateKey()
pub = getPublicKey(priv)
print("Private:")
storePrivateKey(priv)
print("Public:")
storePublicKey(pub)
newpriv = openPrivateKey()
newpub = openPublicKey()
privatepem = newpriv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption() 
    )
print(privatepem)
print("Enter your message:")
message = str(input())
if len(message) > 0:
    print (message)
    secret = encrypt(message, pub)
else:
    print ("empty")

decrypt(priv, secret)

#ToDo: if statement ob private key vorhanden ist
#ToDo: if statement ob public key vorhanden ist
#ToDo: prüfen ob die keys vorhanden sind, wenn ja dann auslesen, wenn nein dann generieren und speichern