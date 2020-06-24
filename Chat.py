import socket
import threading
import time

print("Enter own port:")
port = int(input())
print("Enter partner ip: ")
partner_ip = "localhost"
print("Enter partner port:")
partner_port = int(input())

chat_log = ""
listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This socket listens for the other peer's messages
sending_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This socket sends messages to the other peer


def listening():
    """Listens to incoming Messages from Partner."""
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind(('localhost', port))
    listening_socket.listen(1)
    (partner_connection, address) = listening_socket.accept()
    while True:
        try:
        # Receive Message from Partner
            data = partner_connection.recv(1024)
        except socket.error:
            partner_connection.close()
        # Parse Message    
        message = str(data, "utf-8")
        if message == "EXIT":
            partner_connection.close()
            print("Partner disconnected.")
        print(f"Partner says: {message}")

def sending(partner_ip:str, partner_port:int):
    """Sends Messages to Partner."""
    print(f"Partner ip and port: {partner_ip}:{partner_port}")

    # Connect to Partner
    while True:
        try:
            print("Trying to connect to partner")
            sending_socket.connect((partner_ip, partner_port))
            break
        except:
            time.sleep(3)
            continue
    print("Successfully connected!")
    print("Enter Message:")
    # Send Messages
    while True:
        
        message = input()
        if message == "" or message == "EXIT":
            sending_socket.close()
            break
        data = bytes(message, "utf-8")
        try:
            sending_socket.send(data)
            print(f"You Said: {message}")
        except socket.error:
            sending_socket.close()
            break

def start_chat(partner_ip: str, partner_port: int):
    # Starting listening socket
    listening_thread = threading.Thread(target=listening, daemon=True)
    listening_thread.start()
    sending_thread = threading.Thread(target=sending, args=(partner_ip, partner_port), daemon=True)
    sending_thread.start()
    sending_thread.join()
    listening_thread.join()

start_chat(partner_ip, partner_port)
