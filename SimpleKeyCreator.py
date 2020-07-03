import base64
import cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


#how to generate a simple key
public_key = Fernet.generate_key()
print("\nThis is your randomly generated public Key:\n" + str(public_key,"utf-8") + "\n")

#getting a key based on an input
print("Please enter a password to get your private key:")
password_provided = str(input())
print("\n")
password = password_provided.encode() #str to bytes
#x = os.urandom(16) #to generate one salt (change for different passwords)
#print (x)
salt = b'\xf7\x10\x04\x18\x1d\x0c\xc9\x13\x90\xca=\x14\x1bt\xa2\xf1' #generates 16byte random number, same salt must be used to produce same result
#print(salt)
kdf = PBKDF2HMAC( #deriving a key from a password
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,# A salt. Secure values are 128-bits (16 bytes) or longer and randomly generated.
    iterations=10000, #number of iterations to perform of the hash function
    backend=default_backend()
)
P_key = base64.urlsafe_b64encode(kdf.derive(password))
print("Never share this key with anyone and store it safely!\nThis is your password encrypted Key:\n" + str(P_key,"utf-8"))
