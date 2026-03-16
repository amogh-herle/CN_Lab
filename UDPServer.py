import socket
from queue import Queue

localIP = "127.0.0.1"
localPort = 20005
bufferSize = 1024

UDPServerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
UDPServerSocket.bind((localIP, localPort))

print("UDP server up and listening")

# Queue size
BATCH_SIZE = 5
log_queue = Queue(maxsize=BATCH_SIZE)

while True:

    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0].decode()

    # If queue not full → add log
    if not log_queue.full():
        log_queue.put(message)
        print("Log buffered:", message)

    # If queue full → process logs
    if log_queue.full():

        print("\n---- Processing 5 Logs ----")

        logs = []

        while not log_queue.empty():
            logs.append(log_queue.get())

        # Time ordering
        logs.sort()

        for log in logs:
            print(log)

        print("---- Queue cleared ----\n")