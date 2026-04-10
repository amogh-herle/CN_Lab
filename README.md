# UDP Load Balancer with Log Aggregation

A distributed logging system that demonstrates streaming ingestion, time-based ordering, backpressure handling, and throughput evaluation using UDP communication.

## Overview

This project implements a load-balanced log aggregation system with three main components:

- **Clients**: Generate and send log entries to the load balancer
- **Load Balancer**: Distributes incoming logs across multiple servers using least-connections algorithm
- **Servers**: Process logs, apply backpressure when overloaded, and forward sorted logs back to the load balancer

## Features

### 1. Streaming Ingestion
Continuously collects logs in real-time from multiple client sources without blocking.

### 2. Time-Based Ordering
Logs from different machines are sorted by timestamp before being written to the aggregated output file.

### 3. Backpressure Handling
When server buffers reach capacity thresholds:
- **SLOW_DOWN**: Sent at 80% capacity - clients reduce their sending rate
- **STOP**: Sent at 100% capacity - clients pause temporarily before retrying

Clients dynamically adjust their sending intervals based on these signals and gradually recover when conditions improve.

### 4. Throughput Evaluation
Real-time monitoring of:
- Logs forwarded per second
- Per-server processing rates
- Buffer depths and backpressure events
- Active connections per server

## Architecture

```
Clients (n) <--> Load Balancer <--> Servers (m)
                     |
                     v
            aggregated_logs.txt
```

- Clients send logs to the load balancer on port 22000
- Load balancer forwards to servers on ports 20000, 20001, etc.
- Servers send processed logs and signals back to load balancer on port 21000
- Load balancer aggregates and writes time-ordered logs to file



```bash
git clone <repository-url>
cd CN_Lab
```

## Usage

### 1. Start the Servers

Launch multiple server instances:

```bash
python RunServer.py 3
```

This starts 3 servers on ports 20000, 20001, 20002.

Options:
- `--start-port`: First port number (default: 20000)
- `--lb-port`: Load balancer port for receiving logs (default: 21000)
- `--host`: Server bind IP
- `--lb-host`: Load balancer IP

### 2. Start the Load Balancer

```bash
python UDPLoadBalancer.py --servers 20000 20001 20002
```

Options:
- `--host`: Load balancer bind IP (default: 192.168.137.1)
- `--client-port`: Port for client connections (default: 22000)
- `--lb-port`: Port for server responses (default: 21000)
- `--server-host`: Backend server IP
- `--servers`: List of server ports

### 3. Start the Clients

Launch multiple client instances:

```bash
python RunClient.py 4
```

This starts 4 clients that send logs at 1-second intervals.

Options:
- `--host`: Load balancer IP
- `--port`: Load balancer client port (default: 22000)
- `--interval`: Seconds between log sends (default: 1.0)
- `--client-ip`: Client machine IP for control commands

### Individual Client Usage

Run a single client with interactive controls:

```bash
python Udpclient.py --name Machine-A --interval 1.0
```

Interactive commands:
- `s`: Start/pause sending
- `f`: Toggle rapid fire mode
- `n`: Change machine name
- `i`: Change send interval
- `r`: Set retry duration after STOP
- `h`: Show help
- `q`: Quit

## Configuration

### Server Buffer Tuning

In `Udpserver.py`, adjust the buffer size based on your load:

```python
n = 50  # Total buffer capacity
FLUSH_DELAY = 0.1  # Seconds between log flushes
```

Recommended buffer size formula:
```
n = (incoming_rate / drain_rate) * 3
```

Where:
- `incoming_rate = num_clients / send_interval`
- `drain_rate = 1 / FLUSH_DELAY`

### Client Recovery Settings

In `Udpclient.py`:

```python
MAX_INTERVAL = 16.0  # Maximum backoff interval
RECOVERY_STEP_DELAY = 10.0  # Seconds between recovery steps
```

## Output

### aggregated_logs.txt

Time-ordered log entries with format:
```
timestamp  level     [machine]  component      message  [Server:port]
09:42:34   INFO      [Machine-1]  AuthService   Request completed in 251ms  [Server:20001]
```

### Console Output

**Load Balancer:**
- Throughput statistics every 3 seconds
- Per-server logs/second and active connections
- Backpressure signal relay notifications

**Servers:**
- Real-time throughput (logs/second)
- Buffer depth and status (OK/SLOW/STOPPED)
- Backpressure events (SLOW_DOWN/STOP signals sent)

**Clients:**
- Backpressure signal reception
- Interval adjustments
- Connection status

## Testing Backpressure

1. Start with a small buffer size (n=20) in servers
2. Launch many clients or use rapid fire mode
3. Observe SLOW_DOWN and STOP signals
4. Watch clients automatically adjust their rates
5. Monitor gradual recovery as load decreases

## Network Configuration

The system is designed for distributed deployment across multiple machines. Update IP addresses in:

- `UDPLoadBalancer.py`: Set `--host` to load balancer IP
- `Udpserver.py`: Set `--host` to server machine IP, `--lb-host` to load balancer IP
- `Udpclient.py`: Set `--host` to load balancer IP

For local testing, use `127.0.0.1` for all components.


