import socket
import time

localIP = "127.0.0.1"
localPort = 20005
bufferSize = 1024

UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

UDPServerSocket.bind((localIP, localPort))

print("UDP server up and listening")

log_buffer = []
MAX_BUFFER = 20

log_count = 0
start_time = time.time()

while True:

    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)

    message = bytesAddressPair[0].decode()

    address = bytesAddressPair[1]

    # ---------- Backpressure Handling ----------
    if len(log_buffer) >= MAX_BUFFER:
        print("⚠ Buffer full. Dropping log.")
        continue

    # ---------- Streaming Ingestion ----------
    log_buffer.append(message)

    print("\nReceived Log:", message)

    # ---------- Time Ordering ----------
    log_buffer.sort()

    print("\nOrdered Logs:")
    for log in log_buffer:
        print(log)

    # ---------- Throughput Evaluation ----------
    log_count += 1
    current_time = time.time()

    if current_time - start_time >= 1:
        print("\n🚀 Throughput:", log_count, "logs/sec")
        log_count = 0
        start_time = current_time