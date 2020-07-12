import ChordNoIP
from time import sleep

port_pre = 10000
local_node = ChordNoIP.LocalNode({}, port_pre+1, "", "IchbinCool")
number_of_nodes = 11
local_nodes = []
for number in range(3,number_of_nodes,2):
    print(f"Number: {number}")
    sleep(3)
    local_nodes.append(ChordNoIP.LocalNode({}, port_pre+number, f"84.137.116.42:{port_pre+1}", str(port_pre+number)))

while True:
    pass