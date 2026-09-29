# Docker-Based Bully & Ring Election Experiment

## Experiment
Bully and Ring Election Algorithms

## Distributed Airline Booking System

This version uses **Docker containers as separate distributed nodes**.

Architecture:

    Client
       |
       v
    Queue Nodes
       |
    +--+--+--+--+
    |  |  |  |
   Q1 Q2 Q3 Q4
       |
   Leader Election

Each queue node is a separate Docker container and communicates
with other containers using TCP sockets over a Docker bridge network.

## Files

- `queue_node.py` — main distributed TCP node
- `docker-compose.yml` — creates four queue-node containers
- `Dockerfile` — builds the Python node image
- `client.py` — simple client reference
- `README.md` — instructions

## Requirements

Install:
1. Docker Desktop
2. VS Code

No Python package installation is required.

Check Docker:

    docker --version

Check Compose:

    docker compose version

## IMPORTANT: Run Bully Algorithm

Open the project folder in VS Code.

Open Terminal:

    docker compose up --build

By default the environment uses:

    ALGORITHM=BULLY

You should see four containers:

    queue1
    queue2
    queue3
    queue4

Initially:

    Queue-4 = Leader

The highest active node has the highest ID.

## Watch the logs

Open another VS Code terminal:

    docker compose logs -f

You can see communication and election messages.

## Simulate leader failure

Open another terminal:

    docker stop queue4

Queue-3 should detect that Queue-4 is unavailable and start the
Bully election.

Expected concept:

    Queue-4 DOWN
        |
        v
    Queue-3 starts/continues election
        |
        v
    No higher active node
        |
        v
    Queue-3 becomes NEW LEADER

## Start Queue-4 again

    docker start queue4

Queue-4 comes back as an active node.

To make Queue-4 become leader again, restart the election environment
or use a new election after recovery. Because Queue-4 has the highest
ID, the Bully algorithm can select it.

## Run Ring Algorithm

First stop the current stack:

    docker compose down

Set the algorithm to RING.

PowerShell:

    $env:ALGORITHM="RING"
    docker compose up --build

Or create a `.env` file containing:

    ALGORITHM=RING

Then:

    docker compose up --build

Now the nodes use the Ring Election Algorithm.

## Ring failure demonstration

Stop Queue-4:

    docker stop queue4

A remaining node detects the missing leader.

The election message travels logically through:

    Queue-1 -> Queue-2 -> Queue-3 -> Queue-1

The active IDs are collected.

Queue-3 has the highest active ID, so:

    Queue-3 = NEW LEADER

## See only Queue-3 logs

    docker logs -f queue3

## See all nodes

    docker compose logs -f

## Stop everything

    docker compose down

If you also want to remove the built containers:

    docker compose down --remove-orphans

## Practical demonstration

### Bully

1. Start with BULLY.
2. Show Queue-4 as leader.
3. Run `docker stop queue4`.
4. Show Queue-3 detecting the failure.
5. Show Queue-3 becoming leader.
6. Explain that the highest active ID wins.

### Ring

1. Stop the current stack.
2. Start with `ALGORITHM=RING`.
3. Show Queue-4 as leader.
4. Run `docker stop queue4`.
5. Show election message moving through the ring.
6. Show Queue-3 becoming leader.
7. Explain that the election message collects active IDs and the highest ID wins.

## Viva explanation

### What is leader election?
Leader election is the process of selecting one active node as the
coordinator/leader in a distributed system.

### Why do we need it?
If the current leader fails, another active node must take over so
the distributed system can continue coordinated operation.

### Bully algorithm
The active node with the highest ID becomes the leader.

### Ring algorithm
Nodes are logically arranged in a ring. An election message moves
around the ring and collects active node IDs. The highest active ID
becomes leader.

### Why Docker?
Docker lets us run multiple isolated containers on one laptop.
Each container behaves like a separate distributed node and
communicates through the Docker network.

## Relation to Airline Booking System

In the project, queue/booking nodes can use leader election when the
current coordinator fails.

Example:

    Client
       |
    Load Balancer
       |
    Queue-1 Queue-2 Queue-3 Queue-4
                       |
                     Leader

If the leader fails:

    Failure
       |
    Election
       |
    New Leader
       |
    Booking system continues

This experiment demonstrates the leader-election component. It can
later be integrated with the booking, queue, load-balancing and
fault-tolerance layers.
