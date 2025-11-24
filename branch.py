import grpc
import banks_pb2, banks_pb2_grpc

class Branch(banks_pb2_grpc.BranchServiceServicer):  
    
    # Initialize the branch with ID, balance, and list of branches
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = list()  
        self.log = list()
        self.logical_clock = 0

        # Create stubs to other branches
        for branch_instance in branches:
            if branch_instance["id"] != self.id:
                port_number = branch_instance.get("port", 50050 + branch_instance["id"])
                channel_instance = grpc.insecure_channel(f'localhost:{port_number}')
                stub_instance = banks_pb2_grpc.BranchServiceStub(channel_instance)
                self.stubList.append((branch_instance["id"], stub_instance))

        print(f"[Branch {self.id}] starts with balance_amount={self.balance}")

    # Main handler for incoming requests
    def MsgDelivery(self, request, context):
        # <-- FIX: Interface_type matches proto
        interface_instance = request.Interface_type

        # Update logical clock based on received request
        self.logical_clock = max(self.logical_clock, request.logicalClock) + 1

        # Helper to return a BranchResponse
        def return_response(result="success", balance=None):
            response = banks_pb2.BranchResponse(
                id=self.id,
                Interface_type=interface_instance,
                result=result,
                balance=balance if balance is not None else self.balance,
                logicalClock=self.logical_clock
            )
            return response
        
        if interface_instance == "query": 
            self.observe_event(request.customer_request_id, "query", f"event_recv from customer {request.id}")
            return return_response(result="success", balance=self.balance)
        
        elif interface_instance == "deposit":
            self.observe_event(request.customer_request_id, "deposit", f"event_recv from customer {request.id}")
            self.balance += request.money
            self._propagate_interface_update(request.customer_request_id, request.money, "propagate_deposit")
            return return_response(result="success", balance=self.balance)

        elif interface_instance == "withdraw":
            self.observe_event(request.customer_request_id, "withdraw", f"event_recv from customer {request.id}")
            if self.balance >= request.money:
                self.balance -= request.money
                self._propagate_interface_update(request.customer_request_id, request.money, "propagate_withdraw")
                return return_response(result="success", balance=self.balance)
            return return_response(result="fail", balance=self.balance)
        
        elif interface_instance == "propagate_deposit":
            self.observe_event(request.customer_request_id, "propagate_deposit", f"event_recv from branch {request.id}")
            self.balance += request.money
            return return_response(result="success", balance=self.balance)
        
        elif interface_instance == "propagate_withdraw":
            self.observe_event(request.customer_request_id, "propagate_withdraw", f"event_recv from branch {request.id}")
            self.balance -= request.money
            return return_response(result="success", balance=self.balance)
        
        elif interface_instance == "get_log":
            response = banks_pb2.BranchResponse(
                id=self.id,
                Interface_type=interface_instance,
                result="success",
                balance=self.balance,
                logicalClock=self.logical_clock
            )
            # convert self.log dicts into banks_pb2.Events messages
            for item in self.log:
                response.events_log.add(
                    id=item["id"],
                    customer_request_id=item["customer_request_id"],
                    logicalClock=item["logical_clock"],
                    interface_type=item["interface_type"],
                    comment=item["comment"]
                )
            return response

        else:
            return banks_pb2.BranchResponse(
                id=self.id, Interface_type=interface_instance, result="fail", balance=self.balance
            )
        
    # Observe a single event
    def observe_event(self, customer_request_id, interface_type, comment):
        item = {
            "id": self.id,
            "customer_request_id": customer_request_id,
            "logical_clock": self.logical_clock,
            "interface_type": interface_type,
            "comment": comment
        }
        self.log.append(item)

    # Propagate changes to other branches
    def _propagate_interface_update(self, customer_request_id, money_amount, method_name):
        for (remote_id, stub) in self.stubList:
            try:
                stub.MsgDelivery(banks_pb2.BranchRequest(
                    id=self.id,
                    Interface_type=method_name,  # <-- FIX
                    money=money_amount,
                    customer_request_id=customer_request_id,
                    logicalClock=self.logical_clock + 1
                ))
                self.observe_event(customer_request_id, method_name, f"event_sent from branch {self.id}")
            except Exception as e:
                print(f"[Branch {self.id}] Propagation error: {e}")
