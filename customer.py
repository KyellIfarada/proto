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
        
        # Increment clock for sending event
        self.logical_clock += 1
        
        request = banks_pb2.BranchRequest(
            id=self.id,
            Interface_type = interface_type,
            money=money,
            customer_request_id = customer_request_id,
            logicalClock=self.logical_clock
        )

        # Record the send event
        send_event_log = {
            "customer-request-id": customer_request_id,
            "logical_clock": self.logical_clock,  # underscore
            "interface": interface_type,
            "comment": f"event_sent from customer {self.id}"  # Fixed comment
        }
        self.recvMsg.append(send_event_log)

        # Send Message Request via gRPC based off of interface type to retrieve response
        try:
            response = self.stub.MsgDelivery(request)

            # Update logical clock on receive
            self.logical_clock = max(self.logical_clock, response.logicalClock) + 1

            # Customer event log entry to match output format
            event_log = {
                "customer-request-id": customer_request_id,
                "logical_clock": self.logical_clock,  
                "interface": interface_type,
                "comment": f"event_recv from customer {self.id}"  
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