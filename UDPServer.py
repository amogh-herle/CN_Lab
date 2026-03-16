import socket
from queue import Queue

localIP = "127.0.0.1"
localPort = 20005
bufferSize = 1024

UDPServerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
UDPServerSocket.bind((localIP, localPort))

print("UDP server up and listening")

BATCH_SIZE = 5
log_queue = Queue(maxsize=BATCH_SIZE)

while True:

    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0].decode()

    if not log_queue.full():
        log_queue.put(message)

    if log_queue.full():

        logs = []

        while not log_queue.empty():
            logs.append(log_queue.get())

        logs.sort()

        for log in logs:
            print(log)