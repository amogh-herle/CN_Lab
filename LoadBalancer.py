import socket
import time

loadBalancerIP = "127.0.0.1"
loadBalancerPort = 20001
bufferSize = 1024

server_addresses = [
    ("127.0.0.1", 20005),
    ("127.0.0.1", 20006),
    ("127.0.0.1", 20007)
]

UDPLoadBalancerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
UDPLoadBalancerSocket.bind((loadBalancerIP, loadBalancerPort))

print("Load Balancer up and listening on port", loadBalancerPort)
print("Distributing logs to servers:", server_addresses)

current_server_index = 0
total_logs_forwarded = 0

while True:
    bytesAddressPair = UDPLoadBalancerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0]
    client_address = bytesAddressPair[1]
    
    # Round-robin load balancing
    target_server = server_addresses[current_server_index]
    
    UDPLoadBalancerSocket.sendto(message, target_server)
    
    total_logs_forwarded += 1
    print(f"Forwarded log to Server-{current_server_index + 1} | Total: {total_logs_forwarded}")
    
    current_server_index = (current_server_index + 1) % len(server_addresses)
