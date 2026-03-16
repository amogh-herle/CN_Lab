import socket
import time
import datetime
import random

serverAddressPort = ("127.0.0.1", 20005)

bufferSize = 1024

UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

server_name = "Server" + str(random.randint(1,5))

while True:

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    log_message = f"{timestamp} | {server_name} | Log event generated"

    UDPClientSocket.sendto(log_message.encode(), serverAddressPort)

    time.sleep(random.uniform(0.5,2))