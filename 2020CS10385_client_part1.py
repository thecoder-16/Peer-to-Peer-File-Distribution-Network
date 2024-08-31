import hashlib
import socket
import threading
import time
import random

HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "127.0.0.1"

n = 0
data_size = 0
server_ports = []
client_ports = []
client_data = []

RTT = []
total_RTT = 0

#initial connection ot recieve ports
def initial_rec():
    global n, data_size

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def tcp_conn():
        try:
            client.connect((SERVER, 20001))
        except:
            time.sleep(1)
            tcp_conn()
    tcp_conn()

    ports_data = (client.recv(4096).decode(FORMAT)).split()
    n = int(ports_data[0])
    data_size = int(ports_data[1])

    # store the server and client ports
    for i in range(2*n):
        if(i<n): server_ports.append((int(ports_data[2*i+2]), int(ports_data[2*i+3])))
        else:    client_ports.append((int(ports_data[2*i+2]), int(ports_data[2*i+3])))

    client.send(str(-1).encode())
    client.close()

# ask missing chunks from server
def ask_query(udp_socket: socket.socket, tcp_socket: socket.socket, index: int):
    global client_data, RTT, total_RTT

    udp_socket.settimeout(1)
    x = 0
    y = 0

    # till client has all chunks
    while (len(client_data[index]) < data_size):
        x = random.randint(0, data_size-1)

        # if data is present with client
        if client_data[index].get(x) != None: continue  
        time.sleep(0.005*n)

        # when data is not present, select a port at random to request
        temp = "Chunk_Request " + str(x) + " "+ str(index) + " "

        msgFromServer = ()

        # to know RTT of a particular chunk
        RTT_start_time = time.time()

        def try_reuest():
            nonlocal msgFromServer, y
            y = random.randint(0, n-1)
            udp_socket.sendto(temp.encode(), (SERVER, server_ports[index][0]))
            try:
                # ack from server
                msgFromServer = udp_socket.recvfrom(1024)
            except:
                try_reuest()
        try_reuest()
        while(msgFromServer[0].decode() != "Chunk_Request_Ack"):
            try_reuest()

        # send ack to server using tcp
        temp = "Chunk_Request_Ack_Ack " + str(x) + " "
        tcp_socket.send(temp.encode())

        tcp_socket.settimeout(2)
        try:
            temp =  tcp_socket.recv(2048).decode().split('#')
            if(temp[0] != "Retry" or temp[1] != "Retry"): 
                client_data[index][int(temp[0])] = temp[1] 
        except:
            _ = 0

        tcp_socket.send("OK".encode())

        RTT_end_time = time.time()

        # store RTT of each chunk for a client
        RTT[index][int(temp[0])] = RTT_end_time - RTT_start_time
        total_RTT += RTT_end_time - RTT_start_time

# send ack to server that all data recieved
    temp = "Done Client " + str(index)
    udp_socket.sendto(temp.encode(), (SERVER, server_ports[index][0]))    

    # print(RTT[index])

    filename = "client" + str(index) + ".txt"
    complete_data = ""
    for i in range(data_size):
        complete_data += client_data[index][i]

    print(f"md5 hash of client {index} {hashlib.md5(complete_data.encode()).hexdigest()}")
    # print(f"Time Taken by client {index} {time.time() - start_time} ")

    f = open(filename, "w")
    f.write(complete_data)
    f.close()

    tcp_socket.close()
    udp_socket.close()
    return


# answer queries from the server
def ans_query(udp_socket: socket.socket, tcp_socket: socket.socket, index: int):
    global client_data

    msgFromServer, addr = udp_socket.recvfrom(1024)
    # ack
    udp_socket.sendto("Request recieved".encode(), addr)

    temp = (msgFromServer.decode()).split()

    if(temp[0] == "Close"):
        time.sleep(0.75)
        return

    if(temp[0] != "Chunk_Request_S"): 
        ans_query(udp_socket, tcp_socket, index)

    data_send = ""
    if client_data[index].get(int(temp[1])) == None:    
        data_send = "Not_Present#"
    else:   
        data_send = client_data[index].get(int(temp[1]))

    try:
        tcp_socket.send(data_send.encode())
    except:
        time.sleep(0.75)
        return

    tcp_socket.settimeout(1)
    message = ""
    try:
        message = tcp_socket.recv(1024).decode()
    except:
        if(message != "OK"): 
            _ = 0

    # infinite loop
    # have an ack from server to close this
    ans_query(udp_socket, tcp_socket, index)


# recieve initial chunks and initiate sockets 
def handle(p1, p2, index):
    global client_data
    (port1, port2) = p1
    (port3, port4) = p2


    client_tcp_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn():
        try:
            client_tcp_1.connect((SERVER, port1))
        except:
            tcp_conn()
    tcp_conn()

    client_tcp_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn1():
        try:
            client_tcp_2.connect((SERVER, port2))
        except:
            tcp_conn1()
    tcp_conn1()

    client_udp_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_1.bind((SERVER,port3))

    client_udp_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_2.bind((SERVER,port4))

     # initial data for clients
    num_packets = client_tcp_1.recv(1024).decode()
    client_tcp_1.send("ok".encode())
    for i in range(int(num_packets)):
        incoming_data = client_tcp_1.recv(2048).decode()
        client_tcp_1.send("ok".encode())
        temp = incoming_data.split('#')
        client_data[index][int(temp[0])] = temp[1]

    client_tcp_1.send("done".encode())

    client_thread_1 = threading.Thread(target=ask_query, args=(client_udp_1, client_tcp_1, index))
    client_thread_2 = threading.Thread(target=ans_query, args=(client_udp_2, client_tcp_2, index))
    client_thread_1.start()
    client_thread_2.start()

    client_thread_1.join()
    client_thread_2.join()

threads = []
# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=handle, args=(server_ports[i], client_ports[i], i))
        thread.start()
        threads.append(thread)
    for i in range(n):
        threads[i].join()

def main(): 
    global client_data, RTT
    initial_rec()
    client_data = [dict() for x in range(n)]
    RTT = [dict() for x in range(n)]
    start()

start_time = time.time()
main()
print(f"Time Taken {time.time() - start_time} ")
avg_RTT = total_RTT / ((n-1)*data_size)
print(f"Avg_RTT: {avg_RTT} ")

# for i in range(data_size):
#     temp = 0.0
#     for j in range(n):
#         if RTT[j].get(i) != None:
#             temp += RTT[j][i]
#     temp = temp/(n-1)
#     print(f"{i+1} : {temp}")