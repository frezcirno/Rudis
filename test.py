import socket
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 6379))

def random_key(length):
    import string
    import random
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

for i in range(1000):
    sock.sendall(f'*5\r\n$3\r\nSET\r\n$5\r\n{random_key(5)}\r\n$7\r\nmyvalue\r\n$2\r\nEX\r\n:2\r\n'.encode())
    print(sock.recv(1024))

sock.sendall(b'*1\r\n$4\r\nSAVE\r\n')
sock.sendall(b'*1\r\n$6\r\nBGSAVE\r\n')
sock.sendall(b'*1\r\n$12\r\nbgrewriteaof\r\n')

time.sleep(3)

sock.sendall(b'*1\r\n$4\r\nINFO\r\n')

sock.close()
