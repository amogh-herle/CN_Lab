import socket
import time
import datetime
import random
import sys
import threading
import msvcrt

loadBalancerAddress = ("127.0.0.1", 20001)
bufferSize = 1024
MAX_INTERVAL = 8.0

state = {
    "running": True,
    "rapid": False,
    "interval": 1.0,
    "base_interval": 1.0,
    "retry_after": 5.0,
    "stopped": False,
    "typing": False,
    "exit": False
}

def step_down():
    """Halve the interval toward base after recovering from backpressure."""
    current = state["interval"]
    base = state["base_interval"]
    if current > base:
        state["interval"] = max(base, current / 2)
        print(f"  [recovering] interval -> {state['interval']:.1f}s")

def run_client(client_id):
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocket.settimeout(0.3)
    
    client_name = f"Client-{client_id}"
    
    print(f"[{client_name}] Started and sending logs to Load Balancer")
    
    while not state["exit"]:
        if state["typing"] or not state["running"]:
            time.sleep(0.1)
            continue
        
        if state["stopped"]:
            retry = state["retry_after"]
            print(f"  [{client_name}] STOP received - waiting {retry}s before retry...")
            time.sleep(retry)
            state["stopped"] = False
            step_down()
            continue
        
        timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_message = f"{timestamp} | {client_name} | Log event generated"
        
        UDPClientSocket.sendto(log_message.encode(), loadBalancerAddress)
        
        # Listen for backpressure signals
        try:
            data, _ = UDPClientSocket.recvfrom(64)
            signal = data.decode().strip()
            if signal == "STOP":
                state["stopped"] = True
                state["rapid"] = False
                print(f"  [{client_name}] STOP received from server")
            elif signal == "SLOW_DOWN":
                state["interval"] = min(state["interval"] * 2, MAX_INTERVAL)
                state["rapid"] = False
                print(f"  [{client_name}] SLOW_DOWN received - interval -> {state['interval']:.1f}s")
        except socket.timeout:
            # No signal = server is healthy, step down gradually
            step_down()
        
        sleep_time = 0.05 if state["rapid"] else state["interval"]
        time.sleep(sleep_time)
    
    UDPClientSocket.close()

def print_help():
    print("\n  [s] start / pause")
    print("  [f] toggle rapid fire")
    print("  [i] set send interval")
    print("  [r] set retry duration after STOP")
    print("  [h] help")
    print("  [q] quit\n")

def prompt(label):
    state["typing"] = True
    time.sleep(0.15)
    val = input(label).strip()
    state["typing"] = False
    return val

def input_loop():
    print_help()
    while not state["exit"]:
        if msvcrt.kbhit():
            ch = msvcrt.getwch()
            
            if ch in ('q', '\x03'):
                print("\n  Exiting all clients...")
                state["exit"] = True
                break
            
            elif ch == 's':
                state["running"] = not state["running"]
                print(f"  [{'running' if state['running'] else 'paused'}]")
            
            elif ch == 'f':
                state["rapid"] = not state["rapid"]
                print(f"  [rapid fire {'ON -- hammering server' if state['rapid'] else 'OFF'}]")
            
            elif ch == 'i':
                val = prompt("  Interval (seconds): ")
                try:
                    iv = max(0.1, float(val))
                    state["interval"] = iv
                    state["base_interval"] = iv
                    print(f"  interval -> {iv}s")
                except ValueError:
                    print("  invalid")
            
            elif ch == 'r':
                val = prompt("  Retry after STOP (seconds): ")
                try:
                    state["retry_after"] = max(1.0, float(val))
                    print(f"  retry -> {state['retry_after']}s")
                except ValueError:
                    print("  invalid")
            
            elif ch == 'h':
                print_help()
        
        time.sleep(0.1)

if __name__ == "__main__":
    num_clients = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    
    print(f"Starting {num_clients} clients...")
    print(f"Load Balancer: {loadBalancerAddress[0]}:{loadBalancerAddress[1]}")
    print(f"Send interval: {state['interval']}s | Max backoff: {MAX_INTERVAL}s")
    
    threads = []
    for i in range(1, num_clients + 1):
        thread = threading.Thread(target=run_client, args=(i,))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    try:
        input_loop()
    except KeyboardInterrupt:
        print("\n  Exiting all clients...")
        state["exit"] = True
    
    time.sleep(0.5)
    print("All clients stopped.")
