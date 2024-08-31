import hashlib
import random
import threading

5:  0.4466888904571533
10: 1.2979216575622559 1.4529056549072266
15: 1.236565113067627  1.5947792530059814
20: 1.792374610900879
30: 1.8806986808776855
40: 2.4385862350463867
50: 2.851842164993286

**new**

5: 4.63797926902771
10: 6.939901113510132
20: 12.689455032348633
30: 18.44057536125183
40: 23.92092537879944

# port_num = int(3000)
#     for i in range(n):
#         # temp1 = str(random.randint(1024, 49000))
#         # temp2 = str(random.randint(1024, 49000))
#         temp1 = port_num
#         port_num += 1
#         temp2 = port_num
#         port_num += 1
#         server_ports.append((temp1, temp2))
#         t += " " + str(temp1) + " " + str(temp2)
#     for i in range(n):
#         # temp1 = str(random.randint(1024, 49000))
#         # temp2 = str(random.randint(1024, 49000))
#         temp1 = port_num
#         port_num += 1
#         temp2 = port_num
#         port_num += 1
#         client_ports.append((temp1, temp2))
#         t += " " + str(temp1) + " " + str(temp2)

# n=5
# for i in range(n):
#   print(i)
#   if i%2 == 0: 
#     i=i-2
#     print(f" {i} cxedc")
#     continue

import concurrent.futures

def foo(bar):
    print('hello {}'.format(bar))
    return 'foo'

with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(foo, 'world!')
    return_value = future.result()
    print(return_value)

#This code was contributed by Sachin Negi


# data = []

# def set():
#     global data

#     data[3][1]="hello1"
#     data[2][1]="hello2"
#     data[4][1]="hello3"
#     data[1][4]="hello4"
#     data[2][4]="hello5"
#     data[3][4]="hello6"

# def main():
#     global data
#     data = [dict() for x in range(10)]
#     print(data)
#     set()
#     print(data)

# main()


# def out():
#     x = ()
#     y=5
#     print("FCDS")
#     def inn():
#         nonlocal x
#         x = ("Xes", y)
#         print("jiji")
#     inn()
#     print(x)
# out()

# def rand():
#     x = 0
#     while x != 10 : 
#         x = random.randint(1, 10)
#         print(x)
# rand()


# data = []

# def read_file(f):
#     while True:
#         data = f.read(1024)
#         if not data:
#             break
#         yield data

# with open("./A2_small_file.txt", 'r') as f:
#     for piece in read_file(f):
#         data.append(hashlib.md5(piece.encode()).hexdigest())

# print(data)   

# def fetch1(index):
#     print(f"2nd level thread {index}")

# def fetch(index):
#     print(f"1st level thread {index}")
#     y = threading.Thread(target=fetch1, args=(index*index,))
#     y.start()
#     y = threading.Thread(target=fetch1, args=(index*index*index,))
#     y.start()

# for i in range(3):   # for connection 1
#     x = threading.Thread(target=fetch, args=(i,))
#     # threads.append(x)
#     x.start()