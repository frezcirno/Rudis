import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 6379))


def random_key(length):
    import string
    import random

    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


for i in range(10000):
    sock.sendall(
        f"*3\r\n$3\r\nSET\r\n$5\r\n{random_key(5)}\r\n$7\r\nmyvalue\r\n".encode()
    )
    print(sock.recv(1024))


sock.close()
