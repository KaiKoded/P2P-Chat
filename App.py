from appJar import gui
import Chat
import ChordNoIP
import threading
import json
from os import path


class App_UI(object):
    gui = gui("Threading Chord")
    chat_content = ""
    input_ready = False
    port = 11111
    quit = False
    entry_address = ""
    username = ""
    friend_name = ""
    connected = False
    conn_or_socket = {}
    threads = []
    socket = {}
    friend_list = []
    window_name = ""

    def __init__(self):
        super().__init__()
        if not path.exists("friend_list.txt"):
            return
        with open('friend_list.txt') as json_file:
            self.friend_list = json.load(json_file)

    def read_message(self, friend_name: str, message: str):
        self.chat_content = app.chat_content + "\n" + f"{friend_name} says: {message}"
        self.gui.setMessage("chat_output", self.chat_content)

    def chat(self, remote_name):
        self.friend_name = remote_name
        print(f"Starting chat")
        thread = threading.Thread(target=Chat.start, args=(self, self.conn_or_socket), daemon=True)
        thread.start()
        window_name = f"Chat with {self.friend_name}"
        self.window_name = window_name
        self.gui.startSubWindow(window_name, transient=True, blocking=True)
        self.gui.startLabelFrame("Chat")
        self.gui.addEmptyMessage("chat_output")
        self.gui.addEntry("chat_input")
        self.gui.addButtons(["Send"], Chat.chat_button)
        self.gui.stopLabelFrame()
        self.gui.stopSubWindow()
        self.gui.showSubWindow(window_name)
        self.window_name = ""
        self.gui.destroyAllSubWindows()
        print("Chat closed")
        self.connected = False
        self.conn_or_socket.close()
        print("Waiting for all chats to close")
        thread.join()
        print("chat Main Threads closed")
        if self.socket:
            self.socket.close()
            self.socket = {}
        self.chat_content = ""

    def add_friend(self, friend_name: str):
        print("Adding Friend")
        self.friend_list.append(self.friend_name) if self.friend_name not in self.friend_list else self.friend_list
        self.gui.changeOptionBox("Friend List", self.friend_list)
        with open("friend_list.txt", 'w') as outfile:
            json.dump(app.friend_list, outfile)


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
    app.gui.startTabbedFrame("TabbedFrame")

    app.gui.startTab("Direct Connect")
    app.gui.addLabelEntry("Username of Friend")
    app.gui.addButtons(["Connect"], connect_to_friend)
    app.gui.stopTab()

    app.gui.startTab("Friend List")
    app.gui.addLabelOptionBox("Friend List", app.friend_list)
    app.gui.addButtons(["Connect to selected Friend"], connect_to_friend_from_list)
    app.gui.stopTab()

    app.gui.stopTabbedFrame()
    app.gui.go()


def connect_to_friend_from_list(button):
    global app
    global local_node
    friend_name = app.gui.getOptionBox("Friend List")
    app.gui.setEntry("Username of Friend", friend_name)
    connect_to_friend(button)


def connect_to_friend(button):
    global app
    global local_node
    app.friend_name = app.gui.getEntry("Username of Friend")
    app.add_friend(app.friend_name)

    hashed_username = local_node.hash_username(app.friend_name)
    peer_ip, peer_port, ring_pos = local_node.succ(hashed_username).split("_")
    query_response = local_node.query(hashed_username, (peer_ip, int(peer_port)))
    if query_response == "ERROR":
        app.gui.warningBox("Wrong Username", f"No such User {app.friend_name}")
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
