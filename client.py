"""
Simple client for checking the status of Docker queue nodes.

Usage:
    python client.py
"""

import json
import socket
import sys

NODES = {
    1: "localhost",
    2: "localhost",
    3: "localhost",
    4: "localhost",
}

# Docker containers use port 5000 internally.
# This client is mainly included as a simple reference.
# For status checking from inside the Docker network, use:
# docker exec queue1 python -c "..."
#
# The easiest demonstration is to inspect:
# docker compose logs -f
#
# This file is intentionally simple.
def check_localhost(port):
    try:
        with socket.create_connection(("localhost", port), timeout=2) as sock:
            sock.sendall((json.dumps({"type": "GET_STATUS"}) + "\n").encode())
            data = sock.recv(4096).decode().strip()
            print(data)
    except Exception as exc:
        print(f"Could not connect to localhost:{port}: {exc}")


if __name__ == "__main__":
    print("This project uses Docker containers with internal port 5000.")
    print("Use 'docker compose logs -f' to observe the election.")
    print()
    print("For a real client endpoint, expose one container port in")
    print("docker-compose.yml or add a dedicated API gateway.")
