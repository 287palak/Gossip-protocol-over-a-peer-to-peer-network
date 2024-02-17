import socket
import threading

MY_IP = "0.0.0.0"                                               # IP for listening
PORT = int(input())                                             # Port No. for listening               
peer_list = []                                                  # List to store connected Peers            

# Write the outputs to the file
def write_output_to_file(output):
    try:
        file = open("outputseed.txt", "a")  
        file.write(output + "\n") 
    except:
        print("Write Failed")
    finally:
        file.close()

# Create socket to connect 2 computers
def create_socket():
    try:
        global socket
        socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    except:
        print("Socket Creation Error")

# Bind socket
def bind_socket():
    try:
        global socket
        ADDRESS = (MY_IP, PORT)
        socket.bind(ADDRESS)
    except:
        print("Socket Binding Error")
        bind_socket()

# It take list of connected peer and convert it to comma separated string to send it through socket
def list_to_string(peer_list):  
    PeerList = ","  
    for i in peer_list:  
        PeerList += i + ","    
    return PeerList  

# Take the dead node message, find address of dead node from it and remove it from peer list if present
def remove_dead_node(message):
    print(message)
    write_output_to_file(message)
    message = message.split(":")
    dead_node = str(message[1]) + ":" + str(message[2])
    if dead_node in peer_list:
        peer_list.remove(dead_node)

# To handle different peers in threads.It receives peer address from peer and add it to its peer list
# If it receives dead node message then pass it remove_dead_node()
def handle_peer(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')      
            if message:
                if "Dead Node" in message[0:9]:
                    remove_dead_node(message)
                else:
                    message = message.split(":")
                    peer_list.append(str(addr[0])+":"+str(message[1]))
                    output = "Received Connection from " + str(addr[0])+":"+str(message[1])
                    print(output)
                    write_output_to_file(output)
                    PeerList = list_to_string(peer_list)
                    conn.send(PeerList.encode('utf-8'))
        except:
            break
    conn.close()

# To listen at a particular port and create thread for each peer  
def begin():
    socket.listen(5)
    print("Seed is Listening")
    while True:
        conn, addr = socket.accept()
        socket.setblocking(1)
        thread = threading.Thread(target=handle_peer, args=(conn,addr))
        thread.start()
        
create_socket()
bind_socket()
begin()