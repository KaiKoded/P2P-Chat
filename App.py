from appJar import gui
import Chat
import ChordNoIP
import socket
import time
import threading

class App_UI(object):
    gui = gui("Threading Chord")
    chat_content = ""
    input_ready = False
    port = 11111
    quit = False
    entry_address = "",
    username = ""
    friend_name = ""
    connected = False
    conn_or_socket = {}
    threads = []
    socket = {}

    def read_message(self, friend_name: str, message: str):
        self.chat_content = app.chat_content + "\n" + f"{friend_name} says: {message}"
        self.gui.setMessage("chat_output", self.chat_content)

    def chat(self, remote_name):
        self.friend_name = remote_name
        print(f"Starting chat")
        thread = threading.Thread(target=Chat.start, args=(self, self.conn_or_socket), daemon=True)
        thread.start()
        window_name = f"Chat with {self.friend_name}"
        self.gui.startSubWindow(window_name, transient=True, blocking=True)
        self.gui.startLabelFrame("Chat")
        self.gui.addEmptyMessage("chat_output")
        self.gui.addEntry("chat_input")
        self.gui.addButtons(["Send"], Chat.chat_button)
        self.gui.stopLabelFrame()
        self.gui.stopSubWindow()
        self.gui.showSubWindow(window_name)
        print("Chat closed")
        self.connected = False
        self.conn_or_socket.close()
        print("Waiting for all chats to close")
        thread.join()
        print("chat Main Threads closed")
        self.gui.destroyAllSubWindows()
        if self.socket:
            self.socket.close()
            self.socket = {}
        self.chat_content = ""

        
def login(button):
    global app
    app.username = app.gui.getEntry("Username")
    app.port = int(app.gui.getEntry("Port"))
    app.entry_address = app.gui.getEntry("EntryPoint")
    if len(app.entry_address) != 0 and len(app.entry_address.split(":")) < 2:
        app.entry_address = f"127.0.0.1:{app.entry_address}"
        print(f"Entry Adress: {app.entry_address}")
    connect_to_overlay(app)
        
def connect_to_overlay(app):
    app.gui.stop()

    global local_node
    local_node = ChordNoIP.LocalNode(app=app, port=app.port, entry_address=app.entry_address, username=app.username)

    app.gui = gui(f"{app.username} Chat")
    app.gui.addLabelEntry("Friend to connect")
    app.gui.addButtons(["Connect"], connect_to_friend)
    app.gui.go()


def connect_to_friend(button):
    global app
    global local_node
    app.friend_name = app.gui.getEntry("Friend to connect")
    hashed_username = local_node.hash_username(app.friend_name)
    peer_ip, peer_port, ring_pos = local_node.succ(hashed_username).split("_")
    query_response = local_node.query(hashed_username, (peer_ip, int(peer_port)))
    if query_response == "ERROR":
        app.gui.warningBox("Wrong Username", "No such User")
    else:
        friend_ip, friend_port = query_response 
        print("connect_to_friend() : " + friend_ip + ":" + str(friend_port))
        local_node.start_chat(friend_ip, int(friend_port))


app = App_UI()
local_node = {}

app.gui.addLabel("title", "P2P Chat")
app.gui.addLabelEntry("Username")
app.gui.addLabelEntry("Port")
app.gui.addLabelEntry("EntryPoint")

app.gui.addButtons(["Login"], login)
app.gui.go()
app.gui.destroyAllSubWindows()
if local_node:
    local_node.shutdown()
