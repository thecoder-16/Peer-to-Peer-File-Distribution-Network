import socket
import string 
import threading
import hashlib
import random
import time
from collections import OrderedDict

n = 5
total_recieved = 0
c = 0
HEADER = 1024
localIP     = "127.0.0.1"
localPort   = 20001
bufferSize  = 1024
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
data = []
server_ports = []
client_ports = []

socket_list_tcp = []
socket_list_tcp_2 = []

socket_list_udp_2 = []

busy = False
lock = threading.Lock()

class LRUCache:

 # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, k: int) -> string:
        lock.acquire()
        if k not in self.cache:
            lock.release()
            return "-1"
        else:
            self.cache.move_to_end(k)
            lock.release()
            return self.cache[k]

    def put(self, k: int, v: string) -> None:
        lock.acquire()
        self.cache[k] = v
        self.cache.move_to_end(k)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last = False)
        lock.release()  

def read_file(f):
    while True:
        data = f.read(1024)
        if not data:
            break
        yield data

def split_file(file_name, chunk_size):
    c = 0
    with open(file_name) as f:
        chunk = f.read(chunk_size)
        while chunk:
            data.append(chunk)
            chunk = f.read(chunk_size)

#initial connection to transfer ports
def initial_send():
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, localPort))
    TCPServerSocket.listen(1)

    connectionSocket, addr = TCPServerSocket.accept()

    # randomly select ports for server and clients
    t = str(n) + " " + str(len(data))
    for i in range(n):
        temp1 = str(random.randint(1024, 49000))
        temp2 = str(random.randint(1024, 49000))
        server_ports.append((int(temp1), int(temp2)))
        t += " " + temp1 + " " + temp2
    for i in range(n):
        temp1 = str(random.randint(1024, 49000))
        temp2 = str(random.randint(1024, 49000))
        client_ports.append((int(temp1), int(temp2)))
        t += " " + temp1 + " " + temp2
    
    # send initial data to client
    # initial data includes number of clients and number of chunks in file and all ports to be used
    while True:
        connectionSocket.send(t.encode())

        message = connectionSocket.recv(bufferSize).decode()
        message = int(message)
        
        if message == -1:
            connectionSocket.close()
            TCPServerSocket.close()
            break


# Brodcast request to all clients and return data
def handle_request(client_id: int, packet_id: int, UDPServerSocket_2: socket.socket) -> string:

    for i in range(n):

        if(i == client_id): continue
        message = "Chunk_Request_S#" + str(packet_id) + "#" + str(UDPServerSocket_2.getsockname()) + "#"
        socket_list_tcp_2[i].send(message.encode())
        
        # ack from client
        try:
            msgRecieved = socket_list_tcp_2[i].recv(1024)
        except:
            continue

        try:
            data_recieved, addr = UDPServerSocket_2.recvfrom(1024).decode().split('#')
        except:
            continue
        UDPServerSocket_2.sendto("OK".encode(), addr)

        if(data_recieved[0] == "Not_Present"):
            continue
        else:
            return data_recieved[0]

    return "Retry"


# handles queries from client, brodcast to all clients then send back to client
def handle_client(index: int, TCPServerSocket_1: socket.socket, TCPServerSocket_2: socket.socket, UDPServerSocket_1: socket.socket, UDPServerSocket_2: socket.socket):
    global data, total_recieved, socket_list_tcp

    # listen query from clients
    time.sleep(0.005*n)
    client_req = TCPServerSocket_1.recv(1024)
    request = client_req.decode().split()
    
    if(request[0] == "Done"):
        total_recieved += 1
        return

    temp = "Chunk_Request_Ack#" + str(UDPServerSocket_1.getsockname())
    TCPServerSocket_1.send(temp.encode())

    # check ack from client using tcp and timeout , recurse
    UDPServerSocket_1.settimeout(1)
    try:
        temp, addr = UDPServerSocket_1.recvfrom(1024)
        temp = temp.decode().split()
    except:
        handle_client(index, TCPServerSocket_1, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)

    try:
        _ = int(request[2]) + int(temp[1]) 
    except:
        total_recieved+=1
        return

    # check cache
    data_to_send = cache.get(int(request[1]))
    # if not in cache brodcast request to all clients
    # and update cache
    data_to_send = socket_list_tcp[int(temp[1])]
    cache.put(int(temp[1]), data_to_send)
    data_to_send = "#" + data_to_send + "#"
    data_to_send = request[1] + data_to_send
    def try_send():
        UDPServerSocket_1.sendto(data_to_send.encode('utf-8', 'ignore'), addr)
        try:
            msg, _ = UDPServerSocket_1.recvfrom(1024)
        except:
            try_send()
    try_send()

    try:
        # ack from client
        ax =  UDPServerSocket_1.recv(1024)
    except:
        UDPServerSocket_1.send(data_to_send.encode('utf-8', 'ignore'))

    # recurse
    handle_client(index, TCPServerSocket_1, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)


def send_chunks(port1, port2):
    global c, data, socket_list_tcp, socket_list_tcp_2, socket_list_udp, socket_list_udp_2

    client_id = c

    c+=1
    num_packets = 0
    if(c > len(data)%n): num_packets = int(len(data)/n)
    else: num_packets = int(len(data)/n) + 1

    c1 = int(len(data)/n) * (c-1) + min(c-1, len(data)%n)

    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, port1))
    TCPServerSocket.listen(n)

    connectionSocket, _ = TCPServerSocket.accept()
    # socket_list_tcp.append(connectionSocket)


    TCPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket_2.bind((localIP, port2))
    socket_list_tcp = data
    TCPServerSocket_2.listen(n)
    connectionSocket_2, _ = TCPServerSocket_2.accept()
    socket_list_tcp_2.append(connectionSocket_2)
    connectionSocket_2.settimeout(1)

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, port1))

    UDPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket_2.bind((localIP, port2))
    socket_list_udp_2.append(UDPServerSocket_2)

# initial send
    connectionSocket.send(str(num_packets).encode())
    _ = connectionSocket.recv(1024)
    for i in range(num_packets):
        temp = str(c1) + '#' + data[c1]
        c1+=1
        connectionSocket.send(temp.encode('utf-8', 'ignore'))
        _ = connectionSocket.recv(1024)
    while True:
        msg = connectionSocket.recv(1024).decode()
        if msg == "done": break

    # handles queries from client, brodcast to all clients then send back to client
    handle_client(client_id, connectionSocket, connectionSocket_2, UDPServerSocket, UDPServerSocket_2)

    if(total_recieved == n):
        message = "Close "
        for i in range(n):
            UDPServerSocket_2.sendto(message.encode(), (localIP, client_ports[i][1]))
    # TCPServerSocket.close()
    # TCPServerSocket_2.close()
    # connectionSocket.close()
    # connectionSocket_2.close()
    # UDPServerSocket.close()
    # UDPServerSocket_2.close()


threads = []
# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=send_chunks, args=(server_ports[i]))
        thread.start()
        threads.append(thread)
    for i in range(n):
        threads[i].join()

cache = LRUCache(0)

def main():
    global data, cache
    cache = LRUCache(n)
    file_name = "./A2_small_file.txt"
    # file_name = "./test1.txt"
    split_file(file_name, 1024)

    temp = open("./A2_small_file.txt", 'r')
    # temp = open("./test1.txt", 'r')
    print(hashlib.md5(temp.read().encode()).hexdigest())

    initial_send()
    start()

start_time = time.time()
main()
print(f"Time Taken {time.time() - start_time} ")