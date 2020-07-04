from appJar import gui
# TODO instead of stopping, just clear
app = gui("Peer2Peer Chat", "800x400")
# add & configure widgets - widgets get a name, to help referencing them later
app.addLabel("title", "P2P Chat")
app.addLabelEntry("Username")
app.addLabelEntry("Port")
app.addLabelEntry("EntryPoint")

chat_content = ""


def login(button):
    global app
    if button == "Cancel":
        app.stop()
    else:
        username = app.getEntry("Username")
        port = app.getEntry("Port")
        entry = app.getEntry("EntryPoint")

        connect_to_overlay(entry)
        
def connect_to_overlay(entry):
    #TODO
    global app
    app.stop()
    # TODO Connect to overlay
    app = gui("Peer2Peer Chat", "800x400")
    app.addLabelEntry("Friend to connect")
    app.addButtons(["Connect"], connect_to_friend)
    app.go()

def connect_to_friend():
    global app
    app.stop()
    # TODO Connect to friend peer
    start_chat("TODO")


def start_chat(friend_name):
    global app 
    app = gui("Peer2Peer Chat", "800x400")
    app.addEmptryMessage("chat_output")
    app.addEntry("chat_input")
    app.addButtons(["Send", "Quit"], chatting)
    app.set
    app.go()

def chatting(button):
    global app
    global chat_content
    input = app.getEntry("chat_input")
    app.clearEntry("chat_input")
    chat_content = chat_content + "\n" + f"You wrote: {input}"
    app.setMessage("chat_output", chat_content)
    




app.addButtons(["Login", "Cancel"], login)

app.go()