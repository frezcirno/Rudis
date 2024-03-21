import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 6379))


def encode(*args):
    result = ""
    result += f"*{len(args)}\r\n"
    for arg in args:
        result += f"${len(arg)}\r\n{arg}\r\n"
    return result.encode()


def decode(data: bytes):
    result = []
    while data:
        if data[0] == ord("*"):
            length, data = data.split(b"\r\n", 1)
            length = int(length[1:])
            for _ in range(length):
                sub_result, data = decode(data)
                result.extend(sub_result)
        elif data[0] == ord("$"):
            length, data = data.split(b"\r\n", 1)
            length = int(length[1:])
            result.append(data[:length])
            data = data[length + 2 :]
        elif data[0] == ord("+"):
            result.append(data[1 : data.index(b"\r\n")])
            data = data[data.index(b"\r\n") + 2 :]
        elif data[0] == ord("-"):
            result.append(data[1 : data.index(b"\r\n")])
            data = data[data.index(b"\r\n") + 2 :]
        elif data[0] == ord(":"):
            result.append(int(data[1 : data.index(b"\r\n")]))
            data = data[data.index(b"\r\n") + 2 :]
        else:
            raise ValueError(f"Unknown type: {data}")
    return result, data


while True:
    cmd = input("Enter command: ")
    if cmd == "exit":
        break
    sock.sendall(encode(*cmd.split()))
    print(decode(sock.recv(4096)))

sock.close()
