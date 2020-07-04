import socket
import threading
import time
from appJar import gui

#print("Enter own port:")
#port = int(input())
#print("Enter partner ip: ")
#partner_ip = "localhost"
#print("Enter partner port:")
#partner_port = int(input())

g_app = ""
listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This socket listens for the other peer's messages
sending_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # This socket sends messages to the other peer


def listening(app, port: int):
    """Listens to incoming Messages from Partner."""
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind(('localhost', port))
    listening_socket.listen(1)
    (partner_connection, address) = listening_socket.accept()
    while True:
        if app.quit:
            listening_socket.close()
            break
        try:
        # Receive Message from Partner
            data = partner_connection.recv(1024)
        except socket.error:
            partner_connection.close()
        # Parse Message    
        message = str(data, "utf-8")
        if message == "EXIT" or message == "":
            partner_connection.close()
            print("Partner disconnected.")
        app.chat_content = app.chat_content + "\n" + f"Friend says: {message}"
        app.gui.setMessage("chat_output", app.chat_content)
        print(f"Partner says: {message}")

def sending(app, partner_ip:str, partner_port:int):
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
        if app.quit:
            sending_socket.close()
            break
        if app.input_ready:
            app.input_ready = False
            message = app.gui.getEntry("chat_input")
            app.chat_content = app.chat_content + "\n" + f"You say: {message}"
            app.gui.setMessage("chat_output", app.chat_content)
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
        time.sleep(0.1)

def chat_button(button):
    global g_app
    if button == "Quit":
        g_app.quit = True
        time.sleep(1)
        g_app.gui.stop()
    else:
        g_app.input_ready = True

def start_chat(app, partner_ip: str, partner_port: int):
    # Prepare Chat
    global g_app
    g_app = app
    app.gui = gui("Threading Chord Chat", "800x400")
    app.gui.startLabelFrame("Chat")
    app.gui.addEmptyMessage("chat_output")
    app.gui.addEntry("chat_input")
    app.gui.addButtons(["Send", "Quit"], chat_button)
    app.gui.stopLabelFrame()
    


    # Starting listening socket
    listening_thread = threading.Thread(target=listening, args=(app, app.port), daemon=True)
    listening_thread.start()
    sending_thread = threading.Thread(target=sending, args=(app, partner_ip, partner_port), daemon=True)
    sending_thread.start()
    app.gui.go()
    sending_thread.join()
    listening_thread.join()
    

#start_chat(partner_ip, partner_port)
