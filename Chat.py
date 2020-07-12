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

g_app = {}


def listening(app, conn_or_socket):
    """Listens to incoming Messages from Partner."""
    time.sleep(1)
    conn_or_socket.settimeout(None)
    while app.connected:
        if app.quit:
            break
        try:
        # Receive Message from Partner
            data = conn_or_socket.recv(1024)
        except Exception as msg:
            print(msg)
            print("!!!!!!!!!!!!!!!!!")
            break
        # Parse Message    
        message = str(data, "utf-8")
        if message == "EXIT" or message == "":
            conn_or_socket.close()
            print("Partner disconnected.")
        app.chat_content = app.chat_content + "\n" + f"{app.friend_name} says: {message}"
        app.gui.setMessage("chat_output", app.chat_content)
        print(f"Partner says: {message}")
    print("Chat not listening anymore")
    app.connected = False
    app.chat_content = "Partner Disconnected"
    try:
        app.gui.setMessage("chat_output", app.chat_content)
    except:
        pass

def sending(app, conn_or_socket):
    """Sends Messages to Partner."""
    # Send Messages
    time.sleep(1)
    while app.connected:
        if app.quit:
            print("App quit")
            break
        if app.input_ready:
            app.input_ready = False
            message = app.gui.getEntry("chat_input")
            app.chat_content = app.chat_content + "\n" + f"You say: {message}"
            app.gui.setMessage("chat_output", app.chat_content)
            if message == "" or message == "EXIT":
                break
            data = bytes(message, "utf-8")
            try:
                conn_or_socket.send(data)
                print(f"You Said: {message}")
            except Exception as msg:
                print(msg)
                print("???????!!!!!!!")
        time.sleep(0.1)
    print("Chat not sending anymore")
    app.connected = False
    app.chat_content = "Partner Disconnected"
    try:
        app.gui.setMessage("chat_output", app.chat_content)
    except:
        pass

def chat_button(button):
    global g_app
    if button == "Quit":
        g_app.quit = True
    else:
        g_app.input_ready = True

def start(app, conn_or_socket):
    # Prepare Chat
    global g_app
    g_app = app

    # Starting listening socket
    listening_thread = threading.Thread(target=listening, args=(app, conn_or_socket), daemon=True)
    listening_thread.start()
    sending_thread = threading.Thread(target=sending, args=(app, conn_or_socket), daemon=True)
    sending_thread.start()
    sending_thread.join()
    listening_thread.join()
    print("chat Threads closed")
    print(app.window_name)
    time.sleep(1)
    #if app.window_name != "":
        #app.gui.destroyAllSubWindows()
    #conn_or_socket.close()
    

#start_chat(partner_ip, partner_port)
