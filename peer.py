import socket
import threading
import random
import time
import hashlib

class PeerNode:
    def __init__(self, ip, port, seeds):
        self.ip = ip
        self.port = port
        self.seeds = seeds
        self.connected_peers = set()
        self.messages_sent = set()
        self.ml = {}
        self.liveness_counter = 0
        self.lock = threading.Lock()

    def connect_to_seed(self, seed_ip, seed_port):
        seed_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            seed_socket.connect((seed_ip, seed_port))
            seed_socket.send(f"{self.ip}:{self.port}".encode())
            seed_socket.close()
        except Exception as e:
            print(f"Error connecting to seed node {seed_ip}:{seed_port}: {e}")

    def connect_to_peer(self, peer_ip, peer_port):
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer_socket.connect((peer_ip, peer_port))
            peer_socket.send(f"{self.ip}:{self.port}".encode())
            self.connected_peers.add((peer_ip, peer_port))
            peer_socket.close()
        except Exception as e:
            print(f"Error connecting to peer {peer_ip}:{peer_port}: {e}")

    def connect_to_random_peers(self):
        # Calculate the minimum number of peers to connect to
        min_peers_to_connect = (len(self.seeds) // 2) + 1
        # Determine the maximum number of peers to connect to (limited to 4)
        num_peers_to_connect = min(min_peers_to_connect, 4)

        # Select a random subset of peers to connect to
        #num_peers_to_connect = random.randint(min_peers_to_connect, max_peers_to_connect)
        selected_peers = random.sample(self.seeds, num_peers_to_connect)

        # Connect to selected peers
        for peer_ip, peer_port in selected_peers:
            threading.Thread(target=self.connect_to_peer, args=(peer_ip, int(peer_port))).start()



    def send_gossip_message(self, message):
        # Create a copy of the connected_peers set to avoid modifying it during iteration
        connected_peers_copy = self.connected_peers.copy()
    
        # Broadcast message to all connected peers
        with self.lock:
            for peer_ip, peer_port in connected_peers_copy:
                try:
                    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peer_socket.connect((peer_ip, peer_port))
                    peer_socket.send(message.encode())
                    peer_socket.close()
                except Exception as e:
                    print(f"Error sending message to peer {peer_ip}:{peer_port}: {e}")


    def handle_message(self, message, sender_ip):
    
        # Extract the timestamp and sender IP from the message
        _, timestamp, request_sender_ip = message.split(":")
        reply_message = f"Liveness Reply:{timestamp}:{request_sender_ip}"
        
        # Send the reply message to the sender
        try:
            reply_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reply_socket.connect((sender_ip, self.port))
            reply_socket.send(reply_message.encode())
            reply_socket.close()
            print(f"Liveness reply sent to {sender_ip}")
        except Exception as e:
            print(f"Error sending liveness reply to {sender_ip}: {e}")
    
        # Check if message has been received before
        message_hash = hashlib.sha256(message.encode()).hexdigest()
        if message_hash not in self.ml:
            # Add message to ML
            self.ml[message_hash] = True
            # Broadcast message to all peers except sender
            with self.lock:
                for peer_ip, peer_port in self.connected_peers:
                    if peer_ip != sender_ip:
                        try:
                            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            peer_socket.connect((peer_ip, peer_port))
                            peer_socket.send(message.encode())
                            peer_socket.close()
                        except Exception as e:
                            print(f"Error sending message to peer {peer_ip}:{peer_port}: {e}")
            # Output message to console and file
            with open("outputfile.txt", "a") as f:
                f.write(f"Received message: {message} from {sender_ip}\n")
            print(f"Received message: {message} from {sender_ip}")

    
    def liveness_check(self):
        while True:
            time.sleep(13)  # Check liveness every 13 seconds
            # Perform liveness check for connected peers
            print("Performing Liveliness check")
            with self.lock:
                for peer_ip, peer_port in self.seeds:
                    print("Looping in all connected peers:")
                    print(self.connected_peers)
                    print(peer_ip, " ", peer_port)
                    
                    if (peer_ip, peer_port) in self.connected_peers:
                        try:
                            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            peer_socket.connect((peer_ip, peer_port))
                            peer_socket.send(f"Liveness Request:{time.time()}:{self.ip}".encode())
                            reply = peer_socket.recv(1024).decode()
                            print(reply)
                            if reply.startswith("Liveness Reply"):
                                print(f"Liveness reply received from {peer_ip}:{peer_port}")
                                self.liveness_counter = 0  # Reset liveness counter
                            peer_socket.close()
                        
                        except Exception as e:
                            print(f"Error checking liveness of peer {peer_ip}:{peer_port}: {e}")
                            # Handle dead node - notify seeds and remove from connected peers
                            self.handle_dead_node(peer_ip, peer_port)
                        
                    else:
                        print("Node not connected")
                        

    def handle_dead_node(self, dead_ip, dead_port):
        # with self.lock:
            if (dead_ip, dead_port) in self.connected_peers:
                self.connected_peers.remove((dead_ip, dead_port))
                print("Removed Dead Node ", dead_ip, ":", dead_port)
                # Notify seeds about dead node
                for seed_ip, seed_port in self.seeds:
                    threading.Thread(target=self.notify_seed_dead_node, args=(seed_ip, seed_port, dead_ip, dead_port)).start()
                # Increment liveness counter
                self.liveness_counter += 1
                print(self.liveness_counter)
                # Output dead node message
                if self.liveness_counter >= 3:
                    message = f"Dead Node:{dead_ip}:{dead_port}:{time.time()}:{self.ip}"
                    self.send_dead_node_message(message)


    def notify_seed_dead_node(self, seed_ip, seed_port, dead_ip, dead_port):
        seed_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            seed_socket.connect((seed_ip, seed_port))
            seed_socket.send(f"Dead Node:{dead_ip}:{dead_port}:{time.time()}:{self.ip}".encode())
            seed_socket.close()
        except Exception as e:
            print(f"Error notifying seed node {seed_ip}:{seed_port} about dead node: {e}")

    def send_dead_node_message(self, message):
        # Broadcast dead node message to all seeds
        print("Removed Dead node")
        #with self.lock:
        for seed_ip, seed_port in self.seeds:
            try:
                seed_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                seed_socket.connect((seed_ip, seed_port))
                seed_socket.send(message.encode())
                seed_socket.close()
            except Exception as e:
                print(f"Error sending dead node message to seed node {seed_ip}:{seed_port}: {e}")

    def start(self):
        # Connect to seed nodes to get information about other peers
        for seed_ip, seed_port in self.seeds:
            threading.Thread(target=self.connect_to_seed, args=(seed_ip, int(seed_port))).start()

        self.connect_to_random_peers()

        # Start liveness check thread
        threading.Thread(target=self.liveness_check).start()

        # Generate and broadcast gossip messages
        for _ in range(10):  # Generate 10 messages
            message = f"{time.time()}:{self.ip}:Message"
            self.send_gossip_message(message)
            time.sleep(5)  # Generate message every 5 seconds

    
if __name__ == "__main__":
    # Read seed node addresses from config.csv file
    seed_nodes = []
    with open("config.csv", "r") as f:
        for line in f:
            ip, port = line.strip().split(",")
            seed_nodes.append((ip, int(port)))

    # Specify the IP address and port number for the peer node
    peer = PeerNode("127.0.0.1", 12345, seed_nodes)
    peer.start()
