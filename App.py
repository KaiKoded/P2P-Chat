from appJar import gui
import Chat


class App_UI(object):
    gui = gui("Threading Chord", "800x400")
    chat_content = ""
    input_ready = False
    port = 11111
    quit = False

app = App_UI()
app.gui.addLabel("title", "P2P Chat")
app.gui.addLabelEntry("Username")
app.gui.addLabelEntry("Port")
app.gui.addLabelEntry("EntryPoint")

def login(button):
    global app
    if button == "Cancel":
        app.gui.stop()
    else:
        username = app.gui.getEntry("Username")
        app.port = int(app.gui.getEntry("Port"))
        entry = app.gui.getEntry("EntryPoint")
        connect_to_overlay(entry)
        
def connect_to_overlay(entry):
    #TODO
    global app
    app.gui.stop()
    # TODO Connect to overlay
    app.gui = gui("Peer2Peer Chat", "800x400")
    app.gui.addLabelEntry("Friend to connect")
    app.gui.addButtons(["Connect"], connect_to_friend)
    app.gui.go()

def connect_to_friend():
    global app
    app.gui.stop()
    friend_username = app.gui.getEntry("Friend to connect")
    # TODO Connect to friend peer
    friend_ip, friend_port = friend_username.split(':') # TODO
    Chat.start_chat(app, friend_ip, int(friend_port))

def chatting(button):
    global app
    input = app.gui.getEntry("chat_input")
    app.gui.clearEntry("chat_input")
    app.chat_content = app.chat_content + "\n" + f"You wrote: {input}"
    app.gui.setMessage("chat_output", app.chat_content)


app.gui.addButtons(["Login", "Cancel"], login)
app.gui.go()