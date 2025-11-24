import grpc
import banks_pb2
import banks_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.logical_clock = 0  
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

        interface_type = event.get("interface").lower()
        money = event.get("money", 0)
        customer_request_id = event.get("id",event.get("customer_request_id", 0))
        
        # <-- FIX: Interface_type matches proto
        request = banks_pb2.BranchRequest(
            id=self.id,
            Interface_type = interface_type,
            money=money,
            customer_request_id = customer_request_id,
            logicalClock=self.logical_clock + 1  
        )

        # Send Message Request via gRPC based off of interface type to retrieve response
        try:
            response = self.stub.MsgDelivery(request)

            self.logical_clock = max(self.logical_clock, response.logicalClock) + 1  

            # Customer event log entry to match output format
            event_log = {
                "customer-request-id": customer_request_id,
                "logical-clock": response.logicalClock,
                "interface": interface_type,
                "comment": f"event_recv from customer {response.id}"
            }
            self.recvMsg.append(event_log)

            return response

        except grpc.RpcError as x:
            print(f"[Customer {self.id}] gRPC error whilst {interface_type}: {x}")
            return {"interface": interface_type, "result": "error"}

    def executeEvents(self):
        if not self.stub:
            raise RuntimeError("Stub does not exist.")

        for event in self.events:
            self.executeSingleEvent(event)
            time.sleep(1.0)
        print(f"[Customer {self.id}] All Events Done.\n")

    def getOutputFormat(self):
        """Return output."""
        return {"id": self.id, "type": "customer","events": self.recvMsg}
