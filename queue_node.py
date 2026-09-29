"""
Docker-based Distributed Election Experiment
Bully and Ring Election Algorithms

Each Docker container runs this same program as one queue node.
The nodes communicate using TCP sockets over the Docker network.

Environment variables:
    NODE_ID   = 1..4
    NODE_NAME = queue1..queue4
    ALGORITHM = BULLY or RING

Run with Docker Compose:
    docker compose up --build

Then stop the leader:
    docker stop queue4

If using BULLY, the highest active node becomes leader.
If using RING, the election message travels through the ring.
"""

import json
import os
import socket
import threading
import time

NODE_ID = int(os.getenv("NODE_ID", "1"))
NODE_NAME = os.getenv("NODE_NAME", f"queue{NODE_ID}")
ALGORITHM = os.getenv("ALGORITHM", "BULLY").upper()

PORT = 5000
ALL_NODES = {
    1: "queue1",
    2: "queue2",
    3: "queue3",
    4: "queue4",
}

HEARTBEAT_INTERVAL = 2
LEADER_TIMEOUT = 5
SOCKET_TIMEOUT = 1.5

state_lock = threading.Lock()
leader_id = 4
election_in_progress = False
last_leader_seen = time.time()


def log(message):
    print(f"[{NODE_NAME} | ID={NODE_ID} | {ALGORITHM}] {message}", flush=True)


def node_address(node_id):
    return ALL_NODES[node_id]


def send_message(node_id, message):
    """Send one JSON message to another Docker container."""
    if node_id == NODE_ID:
        return None

    try:
        with socket.create_connection(
            (node_address(node_id), PORT), timeout=SOCKET_TIMEOUT
        ) as sock:
            sock.sendall((json.dumps(message) + "\n").encode())
            sock.settimeout(SOCKET_TIMEOUT)

            data = b""
            while b"\n" not in data:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk

            if data:
                return json.loads(data.decode().strip())

    except (ConnectionRefusedError, TimeoutError, socket.timeout, OSError):
        return None

    except Exception as exc:
        log(f"Communication error with Queue-{node_id}: {exc}")

    return None


def active_nodes():
    """Return IDs of nodes that currently answer a PING."""
    result = []

    for node_id in ALL_NODES:
        if node_id == NODE_ID:
            result.append(node_id)
            continue

        response = send_message(node_id, {"type": "PING", "from": NODE_ID})

        if response and response.get("type") == "PONG":
            result.append(node_id)

    return result


def broadcast(message):
    for node_id in ALL_NODES:
        if node_id != NODE_ID:
            send_message(node_id, message)


def set_leader(new_leader):
    global leader_id, last_leader_seen

    with state_lock:
        leader_id = new_leader
        last_leader_seen = time.time()

    if new_leader == NODE_ID:
        log("***** I AM THE NEW LEADER *****")
    else:
        log(f"New leader announced: Queue-{new_leader}")


# ---------------------------
# BULLY ALGORITHM
# ---------------------------

def bully_election():
    global election_in_progress

    with state_lock:
        if election_in_progress:
            return
        election_in_progress = True

    try:
        log("Starting BULLY election.")

        higher_nodes = [
            node_id
            for node_id in ALL_NODES
            if node_id > NODE_ID and node_id != NODE_ID
        ]

        active_higher = []

        for node_id in higher_nodes:
            response = send_message(
                node_id,
                {"type": "ELECTION", "from": NODE_ID},
            )

            if response and response.get("type") == "OK":
                active_higher.append(node_id)
                log(f"Queue-{node_id} is alive and has higher ID.")

        if active_higher:
            highest = max(active_higher)
            log(
                f"Higher active node exists. "
                f"Queue-{highest} will continue the election."
            )

            # The higher node will normally start its own election.
            # If it does not, ask it to start explicitly.
            send_message(
                highest,
                {"type": "START_ELECTION", "from": NODE_ID},
            )

            time.sleep(3)

            # If no coordinator was received, retry.
            with state_lock:
                still_leader = leader_id

            if still_leader == NODE_ID:
                log("No coordinator received; retrying election.")
                bully_election()
            return

        # No higher active node -> this node wins.
        log("No higher active node found.")
        log("I have the highest active ID.")
        set_leader(NODE_ID)

        broadcast(
            {
                "type": "COORDINATOR",
                "leader": NODE_ID,
            }
        )

        log(f"Coordinator message sent. Queue-{NODE_ID} is LEADER.")

    finally:
        with state_lock:
            election_in_progress = False


# ---------------------------
# RING ALGORITHM
# ---------------------------

def get_next_active_node(start_id):
    """Find the next active node in logical ring order."""
    for offset in range(1, len(ALL_NODES) + 1):
        candidate = ((start_id - 1 + offset) % len(ALL_NODES)) + 1

        if candidate == NODE_ID:
            return candidate

        response = send_message(
            candidate,
            {"type": "PING", "from": NODE_ID},
        )

        if response and response.get("type") == "PONG":
            return candidate

    return None


def ring_election():
    global election_in_progress

    with state_lock:
        if election_in_progress:
            return
        election_in_progress = True

    try:
        log("Starting RING election.")

        next_node = get_next_active_node(NODE_ID)

        if next_node is None or next_node == NODE_ID:
            set_leader(NODE_ID)
            return

        message = {
            "type": "RING_ELECTION",
            "origin": NODE_ID,
            "ids": [NODE_ID],
        }

        log(f"Sending election message to Queue-{next_node}.")
        send_message(next_node, message)

    finally:
        # The coordinator message will tell us the final leader.
        time.sleep(0.5)
        with state_lock:
            election_in_progress = False


def handle_ring_election(message):
    origin = int(message["origin"])
    ids = list(message.get("ids", []))

    if NODE_ID not in ids:
        ids.append(NODE_ID)

    if origin == NODE_ID:
        # Election message completed one full ring.
        winner = max(ids)

        log(f"Election message completed the ring: {ids}")
        log(f"Highest active ID = {winner}")

        set_leader(winner)

        next_node = get_next_active_node(NODE_ID)

        if next_node and next_node != NODE_ID:
            send_message(
                next_node,
                {
                    "type": "RING_COORDINATOR",
                    "origin": NODE_ID,
                    "leader": winner,
                },
            )

        return

    next_node = get_next_active_node(NODE_ID)

    if next_node and next_node != NODE_ID:
        log(
            f"Forwarding election message {ids} "
            f"to Queue-{next_node}."
        )

        send_message(
            next_node,
            {
                "type": "RING_ELECTION",
                "origin": origin,
                "ids": ids,
            },
        )


def handle_ring_coordinator(message):
    leader = int(message["leader"])
    origin = int(message["origin"])

    set_leader(leader)

    if origin == NODE_ID:
        log("Coordinator message completed the ring.")
        return

    next_node = get_next_active_node(NODE_ID)

    if next_node and next_node != NODE_ID:
        send_message(
            next_node,
            {
                "type": "RING_COORDINATOR",
                "origin": origin,
                "leader": leader,
            },
        )


# ---------------------------
# TCP SERVER
# ---------------------------

def handle_client(conn, address):
    try:
        data = b""

        while b"\n" not in data:
            chunk = conn.recv(4096)

            if not chunk:
                break

            data += chunk

        if not data:
            return

        message = json.loads(data.decode().strip())
        msg_type = message.get("type")

        if msg_type == "PING":
            response = {
                "type": "PONG",
                "node": NODE_ID,
            }

        elif msg_type == "GET_STATUS":
            with state_lock:
                current_leader = leader_id

            response = {
                "type": "STATUS",
                "node": NODE_ID,
                "leader": current_leader,
                "algorithm": ALGORITHM,
            }

        elif msg_type == "ELECTION":
            # Bully: every higher node says OK.
            if NODE_ID > int(message.get("from", 0)):
                response = {"type": "OK", "node": NODE_ID}

                # Start our own election asynchronously.
                threading.Thread(
                    target=bully_election,
                    daemon=True,
                ).start()
            else:
                response = {"type": "OK", "node": NODE_ID}

        elif msg_type == "START_ELECTION":
            response = {"type": "STARTED", "node": NODE_ID}

            if ALGORITHM == "BULLY":
                threading.Thread(
                    target=bully_election,
                    daemon=True,
                ).start()

        elif msg_type == "COORDINATOR":
            new_leader = int(message["leader"])
            set_leader(new_leader)
            response = {"type": "ACK", "node": NODE_ID}

        elif msg_type == "RING_ELECTION":
            response = {"type": "FORWARDED", "node": NODE_ID}

            if ALGORITHM == "RING":
                threading.Thread(
                    target=handle_ring_election,
                    args=(message,),
                    daemon=True,
                ).start()

        elif msg_type == "RING_COORDINATOR":
            response = {"type": "ACK", "node": NODE_ID}

            if ALGORITHM == "RING":
                threading.Thread(
                    target=handle_ring_coordinator,
                    args=(message,),
                    daemon=True,
                ).start()

        else:
            response = {"type": "UNKNOWN", "node": NODE_ID}

        conn.sendall((json.dumps(response) + "\n").encode())

    except Exception as exc:
        log(f"Request handling error: {exc}")

    finally:
        conn.close()


def server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(20)

    log(f"TCP server listening on port {PORT}.")
    log(f"Algorithm: {ALGORITHM}")
    log(f"Initial leader: Queue-{leader_id}")

    while True:
        conn, address = server_socket.accept()

        threading.Thread(
            target=handle_client,
            args=(conn, address),
            daemon=True,
        ).start()


# ---------------------------
# LEADER MONITOR
# ---------------------------

def leader_monitor():
    global last_leader_seen

    # Give Docker containers time to start.
    time.sleep(5)

    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        with state_lock:
            current_leader = leader_id

        if current_leader == NODE_ID:
            continue

        response = send_message(
            current_leader,
            {
                "type": "PING",
                "from": NODE_ID,
            },
        )

        if response and response.get("type") == "PONG":
            last_leader_seen = time.time()
            continue

        if time.time() - last_leader_seen >= LEADER_TIMEOUT:
            log(f"Leader Queue-{current_leader} is not responding.")

            if ALGORITHM == "BULLY":
                bully_election()
            elif ALGORITHM == "RING":
                ring_election()

            time.sleep(2)


def main():
    log("Starting distributed queue node...")

    threading.Thread(target=server, daemon=True).start()
    threading.Thread(target=leader_monitor, daemon=True).start()

    # Keep the main process alive.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
