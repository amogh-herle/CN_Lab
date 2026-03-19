import socket
import time
import json
import random
import threading
import sys
import termios
import tty
from datetime import datetime

HOST          = "127.0.0.1"
PORT          = 20005
BUFFER        = 4096

state = {
    "machine":       "Machine-A",
    "interval":      1.0,
    "retry_after":   5.0,    # seconds to wait after a STOP signal
    "running":       True,
    "stopped":       False,  # True when server sent STOP
}

LEVELS     = ["INFO", "INFO", "DEBUG", "WARN", "ERROR"]
COMPONENTS = ["AuthService", "Database", "Cache", "APIGateway", "Scheduler", "FileWatcher"]
MESSAGES   = {
    "INFO":  ["Request completed in {n}ms", "Cache hit ratio {n}%", "Startup OK", "Config reloaded"],
    "DEBUG": ["Query returned {n} rows", "Buffer flushed ({n} bytes)", "Retry attempt {n}/3"],
    "WARN":  ["Slow query: {n}ms", "Memory at {n}%", "Queue depth: {n}"],
    "ERROR": ["Connection timeout", "Write failed: constraint", "Token expired"],
}

def make_log():
    level = random.choice(LEVELS)
    msg   = random.choice(MESSAGES[level]).format(n=random.randint(1, 999))
    return {
        "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "machine":   state["machine"],
        "component": random.choice(COMPONENTS),
        "level":     level,
        "message":   msg,
    }


def print_help():
    print("\n  Keys:")
    print("  [s] — toggle start / pause")
    print("  [n] — set machine name")
    print("  [i] — set send interval (seconds)")
    print("  [r] — set retry duration after STOP (seconds)")
    print("  [q] — quit\n")


def send_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.3)

    while True:
        if not state["running"]:
            time.sleep(0.2)
            continue

        if state["stopped"]:
            retry = state["retry_after"]
            print(f"\rSTOP received — retrying in {retry}s...\r")
            time.sleep(retry)
            state["stopped"] = False
            print(f"\r  ↩ Resuming after {retry}s\r")
            continue

        entry = make_log()
        try:
            sock.sendto(json.dumps(entry).encode(), (HOST, PORT))
            print(f"\r  [{entry['timestamp']}]  {entry['level']:<5}  [{entry['component']}]  {entry['message']}\r")
        except Exception as e:
            print(f"\r  send error: {e}\r")

        # check for signal from server
        try:
            data, _ = sock.recvfrom(64)
            signal  = data.decode().strip()
            if signal == "STOP":
                state["stopped"] = True
            elif signal == "SLOW_DOWN":
                state["interval"] = min(state["interval"] * 2, 8.0)
                print(f"\rSLOW_DOWN — interval → {state['interval']:.1f}s\r")
            elif signal == "SPEED_UP":
                state["interval"] = max(0.5, state["interval"] * 0.75)
                print(f"\rSPEED_UP — interval → {state['interval']:.1f}s\r")
        except socket.timeout:
            pass

        time.sleep(state["interval"])


def input_loop():
    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ch = sys.stdin.read(1)

            if ch == 'q':
                print("\r\n  Exiting.\r\n")
                import os; os._exit(0)

            elif ch == 's':
                state["running"] = not state["running"]
                print(f"\r  [{'resumed' if state['running'] else 'paused'}]\r\n", end="")

            elif ch == 'n':
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                name = input("\r  New machine name: ").strip()
                if name:
                    state["machine"] = name
                    print(f"\r  Name → '{name}'\r")
                tty.setraw(fd)

            elif ch == 'i':
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                val = input("\r  New interval (seconds): ").strip()
                try:
                    state["interval"] = max(0.1, float(val))
                    print(f"\r  Interval → {state['interval']}s\r")
                except ValueError:
                    print("\r  Invalid value\r")
                tty.setraw(fd)

            elif ch == 'r':
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                val = input("\r  Retry duration after STOP (seconds): ").strip()
                try:
                    state["retry_after"] = max(1.0, float(val))
                    print(f"\r  Retry duration → {state['retry_after']}s\r")
                except ValueError:
                    print("\r  Invalid value\r")
                tty.setraw(fd)

            elif ch == 'h':
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                print_help()
                tty.setraw(fd)

    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


print(f"  UDP Log Client | server {HOST}:{PORT}")
print(f"  Machine: {state['machine']} | interval: {state['interval']}s | retry: {state['retry_after']}s")
print_help()

threading.Thread(target=send_loop, daemon=True).start()
input_loop()