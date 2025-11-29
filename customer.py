import json
import sys
import grpc
import banks_pb2
import banks_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = []
        self.stubList = []
        self.set_of_writes = set()

        # Load File to read of branch info for each customer
        load_file = sys.argv[1]

        try:
            with open(load_file, "r") as file:
                database = json.load(file)
        except Exception as x:
            print(f"[Customer] Unable to read input file for use: {x}")
            return

        # Extract only branch entries and initialize port attributes for each customer
        self.branches = [
            item for item in database if item["type"] == "branch"
        ]
        for branch_instance in self.branches:
            branch_id = branch_instance["id"]
            port = 50050 + branch_id
            branch_instance["port"] = port

    # Create a Stub to connect to all branches via ports.
    def createStub(self, branches):
        for branch in branches:
            address = f"localhost:{branch['port']}"
            channel = grpc.insecure_channel(address)
            stub = banks_pb2_grpc.BranchServiceStub(channel)
            self.stubList.append(stub)
            print(f"[Customer {self.id}] Connected to Bank at {address}")

    # find stub idx by branch id
    def locate_stub_by_branch_id(self, branch_id):
        for a, c in enumerate(self.branches):
            if c.get("id") == branch_id:
                return self.stubList[a], c
        return None, None

    # Create a Single event definition for retrieving information from branches.
    def executeSingleEvent(self, event):
        if not self.stubList:
            raise RuntimeError("Stub does not exist.")

        Interface_type = event.get("interface").lower()
        money = event.get("money", 0)

        target_branch_id = event.get("branch", None)
      
        stub, target_branch = self.locate_stub_by_branch_id(target_branch_id)
        if stub is None:
            raise RuntimeError(
                f"Target branch {target_branch_id} not found for Customer {self.id}"
            )

        # send message of customer id, interface type, money amount
        request = banks_pb2.BranchRequest(
            id=self.id,
            Interface_type=Interface_type,
            money=money
        )

        # Include client's set_of_writes and list the set of writes of the customer
        request.set_of_writes.extend(list(self.set_of_writes))

        try:
            response = stub.MsgDelivery(request)

            # Save write_id if present from branch response if write occured on branch
            if response.write_id:
                self.set_of_writes.add(int(response.write_id))

            # Build output
            if Interface_type == "query":
                output = {
                    "interface": "query",
                    "balance": response.balance,
                    "id": response.id
                }
            else:
                output = {
                    "interface": Interface_type,
                    "result": response.result,
                    "id": response.id
                }

            self.recvMsg.append(output)
            print(f"[Customer {self.id}] {Interface_type.upper()} → {output}")

            return output

        except grpc.RpcError as g:
            print(f"[[Customer] {self.id}] gRPC error during {Interface_type}: {g}")
            return {"interface": Interface_type, "result": "error"}

    def executeEvents(self):
        if not self.stubList:
            raise RuntimeError("StubList not found.")

        for event in self.events:
            self.executeSingleEvent(event)
            time.sleep(1.0)

        print(f"[Customer {self.id}] All Events Completed.\n")

    def getOutputFormat(self):
        """Return output."""
        return {"id": self.id, "recv": self.recvMsg}
