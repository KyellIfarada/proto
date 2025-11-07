import grpc
import banks_pb2
import banks_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.stub = None
    #Create a Stub to connect to branch via a port.
    def createStub(self, address='localhost:50051'):
        channel = grpc.insecure_channel(address)
        self.stub = banks_pb2_grpc.BranchServiceStub(channel)
        print(f"[Customer {self.id}] Connected to Bank at {address}")
    #Create a Single event definition for retrieving information from branch.
    def executeSingleEvent(self, event):
        if not self.stub:
            raise RuntimeError("Stub does not exist.")

        Interface_type = event.get("interface").lower()
        money = event.get("money", 0)
        request = banks_pb2.BranchRequest(
            id=self.id,
            Interface_type=Interface_type,
            money=money
        )

        # Send Message Request via gRPC based off of interface type to retrieve response
        try:
            response = self.stub.MsgDelivery(request)
            # Build message based on interface type
            if Interface_type == "query":
                msg = {"interface": "query", "balance": getattr(response, "balance", 0)}
            else:  # deposit or withdraw
                msg = {"interface": Interface_type, "result": getattr(response, "result", "N/A")}
            self.recvMsg.append(msg)
            print(f"[Customer {self.id}] {Interface_type.upper()} → {msg}") 
            return msg
        except grpc.RpcError as e:
            print(f"[Customer {self.id}] gRPC error whilst {Interface_type}: {e}")
            return {"interface": Interface_type, "result": "error"}

    def executeEvents(self):
        if not self.stub:
            raise RuntimeError("Stub does not exist.")

        for event in self.events:
            self.executeSingleEvent(event)
            time.sleep(1.0)
        print(f"[Customer {self.id}] All Events Done.\n")

    def getOutputFormat(self):
        """Return output."""
        return {"id": self.id, "recv": self.recvMsg}
