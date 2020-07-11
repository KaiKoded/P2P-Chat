from appJar import gui
import Chat
import ChordNoIP
import socket
import time
import threading

class App_UI(object):
    gui = gui("Threading Chord", "800x400")
    chat_content = ""
    input_ready = False
    port = 11111
    quit = False
    entry_address = "",
    username = ""
    friend_name = ""
    connected = False
    conn_or_socket = {}

    def read_message(self,friend_name: str, message: str):
        self.chat_content = app.chat_content + "\n" + f"{friend_name} says: {message}"
        self.gui.setMessage("chat_output", self.app.chat_content)

    def chat(self):
        print(f"Starting chat")
        thread=threading.Thread(target=Chat.start, args=(self, self.conn_or_socket), daemon=True)
        thread.start()
        app.gui.stop()
        app.gui = gui("Threading Chord Chat", "800x400")
        app.gui.startLabelFrame("Chat")
        app.gui.addEmptyMessage("chat_output")
        app.gui.addEntry("chat_input")
        app.gui.addButtons(["Send", "Quit"], Chat.chat_button)
        app.gui.stopLabelFrame()
        app.gui.go()

    
def login(button):
    global app
    if button == "Cancel":
        app.gui.stop()
    else:
        app.username = app.gui.getEntry("Username")
        app.port = int(app.gui.getEntry("Port"))
        app.entry_address = app.gui.getEntry("EntryPoint")
        connect_to_overlay(app)
        
def connect_to_overlay(app):
    app.gui.stop()

    global local_node
    local_node = ChordNoIP.LocalNode(app=app, port=app.port, entry_address=app.entry_address, username=app.username)

    app.gui = gui("Peer2Peer Chat", "800x400")
    app.gui.addLabelEntry("Friend to connect")
    app.gui.addButtons(["Connect", "Wait"], connect_to_friend)
    app.gui.go()

def connect_to_friend(button):
    global app
    global local_node
    if button == "Wait":
        wait_for_friend()
    else:
        app.friend_name = app.gui.getEntry("Friend to connect")
        friend_ip, friend_port, ring_pos = local_node.succ(local_node.hash_username(app.friend_name)).split("_")
        local_node.start_chat(friend_ip, int(friend_port))
        app.chat()
    
def wait_for_friend():
    global app
    #app.gui.stop()
    print("Waiting for friend")
    while True:
        if app.connected:
            app.chat()
            break


app = App_UI()
local_node = {}

app.gui.addLabel("title", "P2P Chat")
app.gui.addLabelEntry("Username")
app.gui.addLabelEntry("Port")
app.gui.addLabelEntry("EntryPoint")

app.gui.addButtons(["Login", "Cancel"], login)
app.gui.go()