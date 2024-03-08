import socket
import threading
import random
import time

class SeedNode:
    def __init__(self, ip, port, n):
        self.ip = ip
        self.port = port
        self.peer_list = {}
        self.lock = threading.Lock()
        self.num_seeds = n

    def handle_connection(self, client_socket, addr):
        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            if data.startswith("Dead Node"):
                dead_node_details = data.split(":")[1:]
                self.remove_dead_node(dead_node_details)
            elif data.startswith("Liveness Request"):
                sender_timestamp, sender_ip = data.split(":")[1:]
                self.send_liveliness_reply(client_socket, sender_timestamp, sender_ip, addr[0])
            else:
                self.register_peer(data, addr)

    def register_peer(self, peer_details, addr):
        with self.lock:
            self.peer_list[addr] = peer_details
        output_message = f"Peer {peer_details} registered with seed."
        self.write_to_file(output_message)
        print(output_message)
        print(f"Updated peer list: {self.peer_list}")
        self.write_to_file(f"Updated peer list: {self.peer_list}")

    def remove_dead_node(self, dead_node_details):
        ip, port = dead_node_details[:2]
        with self.lock:
            if (ip, int(port)) in self.peer_list:
                del self.peer_list[(ip, int(port))]
                output_message = f"Removed dead node {ip}:{port}"
                self.write_to_file(output_message)
                print(output_message)
                print(f"Updated peer list: {self.peer_list}")
                self.write_to_file(f"Updated peer list: {self.peer_list}")

    def send_liveliness_reply(self, client_socket, sender_timestamp, sender_ip, receiver_ip):
        # Send liveliness reply with sender timestamp, sender IP, receiver IP
        reply_message = f"Liveness Reply:{time.time()}:{sender_timestamp}:{sender_ip}:{receiver_ip}"
        try:
            client_socket.send(reply_message.encode())
        except Exception as e:
            print(f"Error sending liveliness reply: {e}")

    def write_to_file(self, message):
        with open("outputfile.txt", "a") as f:
            f.write(message + "\n")

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        output_message = f"Seed node started at {self.ip}:{self.port}"
        self.write_to_file(output_message)
        print(output_message)

        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket, addr)).start()

if __name__ == "__main__":
    with open("config.csv", "r") as f:
        seed_nodes = []
        for line in f:
            ip, port = line.strip().split(",")
            seed_node = SeedNode(ip, int(port), len(seed_nodes))
            seed_nodes.append(seed_node)
            threading.Thread(target=seed_node.start).start()