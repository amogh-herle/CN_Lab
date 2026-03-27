# Distributed UDP Log Processing System

A distributed logging system with multiple clients, a load balancer, and multiple servers implementing streaming ingestion, time ordering, backpressure handling, and throughput evaluation.

## Architecture

```
Multiple Clients → Load Balancer → Multiple Servers
```

- **Clients**: Generate timestamped log events
- **Load Balancer**: Distributes logs using round-robin algorithm
- **Servers**: Process logs with 4 core functionalities

## Core Functionalities

1. **Streaming Ingestion**: Continuous log reception via UDP
2. **Time Ordering**: Sorts logs chronologically by timestamp
3. **Backpressure Handling**: Drops logs when buffer reaches capacity (MAX_BUFFER = 20)
4. **Throughput Evaluation**: Measures and displays logs/sec every second

## How to Run

### Step 1: Start Multiple Servers (in separate terminals)

```bash
python UDPServer.py 1 20005
python UDPServer.py 2 20006
python UDPServer.py 3 20007
```

### Step 2: Start Load Balancer

```bash
python LoadBalancer.py
```

### Step 3: Start Multiple Clients

```bash
python UDPClient.py 5
```

This starts 5 clients. You can change the number as needed.

## Configuration

- **Load Balancer Port**: 20001
- **Server Ports**: 20005, 20006, 20007 (configurable in LoadBalancer.py)
- **Buffer Size**: 20 logs per server
- **Client Send Interval**: Random between 0.3-1.5 seconds

## Example Output

**Server Output:**
```
[Server-1] Received Log: 14:23:45.123 | Client-2 | Log event generated
[Server-1] Ordered Logs:
  14:23:43.456 | Client-1 | Log event generated
  14:23:45.123 | Client-2 | Log event generated

[Server-1] 🚀 Throughput: 15 logs/sec
```

**Load Balancer Output:**
```
Forwarded log to Server-1 | Total: 42
Forwarded log to Server-2 | Total: 43
Forwarded log to Server-3 | Total: 44
```
