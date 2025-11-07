import grpc
from concurrent import futures
import json
import time
import banks_pb2_grpc  # updated import
from branch import Branch

def server(input_file):
    
    #Load configuration from JSON
    with open(input_file) as f:
        database = json.load(f)

    # Extract only branch entries
    branches = [interface_type for interface_type in database if interface_type["type"] == "branch"]

    servers = []

    # Start a gRPC server for each branch
    for branch_instance in branches:
        branch_id = branch_instance["id"]
        port = 50050 + branch_id  
        branch_instance["port"] = port     # store port 

        # Create Servers with single-threaded logic  
        server_instance = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        # Register BranchService (matches proto service name)
        banks_pb2_grpc.add_BranchServiceServicer_to_server(
            Branch(branch_id, branch_instance["balance"], branches),
            server_instance
        )

        server_instance.add_insecure_port(f"[::]:{port}")
        server_instance.start()
        servers.append(server_instance)

        print(f"[Server] Branch_instance {branch_id} running on port_number {port}")

    try:
        # Keep servers alive indefinitely
        while True:
            time.sleep(50000)
    except KeyboardInterrupt:
        for s in servers:
            s.stop(0)
        print("[Server] Powering Down")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python server.py <input_file.json>")
    else:
        server(sys.argv[1])
