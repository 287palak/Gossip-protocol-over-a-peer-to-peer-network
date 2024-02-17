import socket
import threading
import time
import hashlib 
import random
from queue import Queue   

no_of_threads = 3             # We have 3 threads each for listen, liveness testing and gossip
job_no = [1, 2, 3]            # We will create 3 jobs in queue for running each thread  
queue = Queue()               # Queue to store our jobs  

MY_IP = "0.0.0.0"                                          # MY_IP will later contain IP Address for listening 
PORT = int(input())                                        # Port for listening 
seeds_addr = set()                                         # To store seed address received from config.txt
peer_set_from_seed = set()                                 # Set used to store different peers address received from seed 
peers_connected = []                                       # To store list of peer objects connected
MessageList = []                                           # To store hash of GOSSIP messages 
connect_seed_addr = []                                     # To store seed address to which peer is connected

# Write the outputs to the file
def write_output_to_file(output):
    try:
        file = open("outputpeer.txt", "a")  
        file.write(output + "\n") 
    except:
        print("Write Failed")
    finally:
        file.close()

# Class of Peer objects 
class Peer: 
    i = 0
    address = ""
    def __init__(self, addr): 
        self.address = addr 

# To find self timestamp
def timestamp():
    time_stamp = time.time()
    return time_stamp

# To generate hash of message
def hash_of_message(message):
    result = hashlib.sha256(message.encode()).hexdigest()
    return result

# Read address of seeds from config file
def read_addr_of_seeds():
    try:
        global seeds_address_list
        file = open("config.txt","r")
        seeds_address_list = file.read()
    except:
        print("Read from config failed")
    finally:
        file.close()

# To calculate n i.e. total no. of seeds and also to find set of all available seeds
def total_available_seeds():
    global seeds_address_list
    temp = seeds_address_list.split("\n")
    for addr in temp:
        if addr:
            addr = addr.split(":")
            addr = "0.0.0.0:" + str(addr[1])
            seeds_addr.add(addr)
    return len(seeds_addr)

# Generate k random numbers in a given range.  
def generate_k_random_numbers_in_range(lower, higher, k):
    random_numbers_set = set()
    while len(random_numbers_set) < k:
        random_numbers_set.add(random.randint(lower,higher))
    return random_numbers_set

# Create socket to connect 2 computers
def create_socket():
    try:
        global sock
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    except:
        print("Socket Creation Error")

# To Bind Socket
def bind_socket():
    try:
        global sock
        ADDRESS = (MY_IP, PORT)
        sock.bind(ADDRESS)
    except socket.error:
        print("Socket Binding Error")
        bind_socket()

# To handle different connected peers in different thread.It recieves messages from peer.
# According to the type of message received take appropriate actions
def handle_peer(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')
            received_data = message
            if message:
                message = message.split(":")
                if "New Connect Request From" in message[0]:      # If its new connection request then check if already 4 peers are not connected then accept the connection
                    if(len(peers_connected) < 4):
                        conn.send("New Connect Accepted".encode('utf-8'))
                        peers_connected.append( Peer(str(addr[0])+":"+str(message[2])) )
                elif "Liveness Request" in message[0]:            # If its liveness request then give back liveness reply              
                    liveness_reply = "Liveness Reply:" + str(message[1]) + ":" + str(message[2]) + ":" + str(MY_IP)
                    conn.send(liveness_reply.encode('utf-8'))
                elif "GOSSIP" in message[3][0:6]:                 # If its gossip message then forward it if its not in ML list
                    forward_gossip_message(received_data)
        except:
            break
    conn.close()

# To listen at a particular port and create thread for each peer                 
def begin():
    sock.listen(5)
    print("Peer is Listening")
    while True:
        conn, addr = sock.accept()
        sock.setblocking(1)
        thread = threading.Thread(target = handle_peer, args = (conn,addr))
        thread.start()
        
# This function receives complete list of peers and set of random index of peers to connect to and connect to them
def connect_peers(complete_peer_list, selected_peer_nodes_index):
    for i in selected_peer_nodes_index:
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            peer_addr = complete_peer_list[i].split(":")
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            sock.connect(ADDRESS)
            peers_connected.append( Peer(complete_peer_list[i]) )
            message = "New Connect Request From:"+str(MY_IP)+":"+str(PORT)
            sock.send(message.encode('utf-8'))
            print(sock.recv(1024).decode('utf-8'))
            sock.close()
        except:
            print("Peer Connection Error")

# This function takes complete list of peers and find a random no. of peers to connect to b/w 1 and 4 and then generate a set of random no. that size    
def join_atmost_4_peers(complete_peer_list):
    if len(complete_peer_list) > 0:
        limit = min(random.randint(1, len(complete_peer_list)), 4)    # Since we wanna connect to atmost 4 random peers we find a random no. of peers to connect b/w 1 and 4
        selected_peer_nodes_index = generate_k_random_numbers_in_range(0, len(complete_peer_list) - 1, limit)  # This generate "limit" no. of index of peers to connect to
        connect_peers(complete_peer_list, selected_peer_nodes_index)

# It take complete peer list separated by comma from each seed and union them all
def union_peer_lists(complete_peer_list):
    global MY_IP
    complete_peer_list = complete_peer_list.split(",")
    complete_peer_list.pop()
    temp = complete_peer_list.pop()
    temp = temp.split(":")
    MY_IP = temp[0]
    for i in complete_peer_list:
        if i:
            peer_set_from_seed.add(i)
    complete_peer_list = list(peer_set_from_seed)
    return complete_peer_list

# This function is used to connect to seed and send our IP address and port info to seed and then receives a list of peers connected to that seed separated by comma
# After finding union it calls join_atmost_4_peers to connect to atmost peers
def connect_seeds():
    complete_peer_list = []  # Initialize complete_peer_list here
    
    for i in range(0, len(connect_seed_addr)):
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_addr = connect_seed_addr[i].split(":")
            ADDRESS = (str(seed_addr[0]), int(seed_addr[1]))
            sock.connect(ADDRESS)
            MY_ADDRESS = str(MY_IP)+":"+str(PORT)
            sock.send(MY_ADDRESS.encode('utf-8'))
            message = sock.recv(10240).decode('utf-8')
            complete_peer_list = union_peer_lists(message)
            for peer in complete_peer_list:
                print(peer)
                write_output_to_file(peer)
            sock.close()
        except:
            print("Seed Connection Error")
    return complete_peer_list
    # join_atmost_4_peers(complete_peer_list)

# This function is used to register the peer to (floor(n / 2) + 1) random seeds
def register_with_k_seeds():   # where k = floor(n / 2) + 1
    global seeds_addr
    seeds_addr = list(seeds_addr)
    seed_nodes_index = generate_k_random_numbers_in_range(0, n - 1, n // 2 + 1)
    seed_nodes_index = list(seed_nodes_index)
    for i in seed_nodes_index:
        connect_seed_addr.append(seeds_addr[i])
    # connect_seeds()
    complete_peer_list = connect_seeds()
    join_atmost_4_peers(complete_peer_list)

# This function takes address of peer which is down. Generate dead node message and send it to all connected seeds
def report_dead(peer):
    dead_message = "Dead Node:" + peer + ":" + str(timestamp()) + ":" + str(MY_IP)
    print(dead_message)
    write_output_to_file(dead_message)
    for seed in connect_seed_addr:        
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_address = seed.split(":")
            ADDRESS = (str(seed_address[0]), int(seed_address[1]))
            sock.connect(ADDRESS)
            sock.send(dead_message.encode('utf-8'))
            sock.close()
        except:
            print("Seed Down ", seed)

# This function generates liveness request and it to all connected peers at interval of 13 sec
# If three consecutive replies are not received then call report_dead()
def liveness_testing():    
    while True:
        liveness_request = "Liveness Request:" + str(timestamp()) + ":" + str(MY_IP)
        print(liveness_request)
        for peer in peers_connected:
            try:                
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(liveness_request.encode('utf-8'))
                print(sock.recv(1024).decode('utf-8'))
                sock.close()  
                peer.i = 0           # If it is able to send liveness req and get reply then start from 0 again so that we check for 3 consecutive failure
            except:                     # This happens when connection fails so count for 3 consecutive failures for given peer
                peer.i = peer.i + 1   
                if(peer.i == 3):                # If three failures then report this peer as dead node and remove from connected peer list
                    report_dead(peer.address)
                    peers_connected.remove(peer)
        time.sleep(13)

# Forward received gossip message to connected peers if its hash is not in Message List to avoid flooding/looping 
def forward_gossip_message(received_message):
    hash = hash_of_message(received_message) 
    if hash in MessageList:        # If hash of received message is already in Message List then dont forward
        pass
    else:
        MessageList.append(str(hash))   # If it is received for 1st time then append it to its ML list
        print(received_message)
        write_output_to_file(received_message)
        for peer in peers_connected:     # Forward gossip message to all connected peers
            try:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(received_message.encode('utf-8'))
                sock.close()
            except:
                continue

# Generate gossip message and send it to connected peers
def generate_send_gossip_message(i):
    gossip_message = str(timestamp()) + ":" + str(MY_IP) + ":" + str(PORT) + ":" + "GOSSIP" + str(i+1) 
    MessageList.append(str(hash_of_message(gossip_message)))  # Add hash of generated gossip to ML list
    for peer in peers_connected:
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            peer_addr = peer.address.split(":")
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            sock.connect(ADDRESS)
            sock.send(gossip_message.encode('utf-8'))       
            sock.close() 
        except:
            print("Peer Down ", peer.address)

# Generate 10 gossip messages at an interval of 5 sec each
def gossip():
    for i in range(10):
        generate_send_gossip_message(i)
        time.sleep(5)

# create_workers(), work(), create_jobs() : Referred from internet
# Source : https://github.com/attreyabhatt/Reverse-Shell/blob/master/Multi_Client%20(%20ReverseShell%20v2)/server.py
# Create Worker Threads
def create_workers():
   for _ in range(no_of_threads):
       thread = threading.Thread(target = work)
       thread.daemon = True
       thread.start()

# Do next job that is in the queue (handle connections, liveness testing, gossip)
def work():
   while True:
       x = queue.get()
       if x == 1:
           create_socket()
           bind_socket()
           begin()
       elif x == 2:
           liveness_testing()
       elif x == 3:
           gossip() 
       queue.task_done()

# Create jobs in queue
def create_jobs():
    for i in job_no:
        queue.put(i)
    queue.join()

read_addr_of_seeds()
n = total_available_seeds()
register_with_k_seeds()                        # Where k = floor(n / 2) + 1

create_workers()
create_jobs()