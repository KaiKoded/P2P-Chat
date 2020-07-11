from appJar import gui 
import threading
from time import sleep

def launch(win):
    app.showSubWindow(win)

def threadingSub():
    sleep(3)
    launch("one")
    sleep(1)
    app.setLabel("l1", "Testing")

app=gui()

# these go in the main window
app.addButtons(["one", "two"], launch)

# this is a pop-up
app.startSubWindow("one", modal=True)
app.addLabel("l1", "SubWindow One")
app.stopSubWindow()

# this is another pop-up
app.startSubWindow("two")
app.addLabel("l2", "SubWindow Two")
app.stopSubWindow()
thread=threading.Thread(target=threadingSub, daemon=True)
thread.start()

app.go()