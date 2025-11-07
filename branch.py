import grpc
import banks_pb2, banks_pb2_grpc

class Branch(banks_pb2_grpc.BranchServiceServicer):  
# Initialize the branch with ID, balance, and list of branches, including stubs to other branches, and list of recieve message.
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = list()  
        self.recvMsg = list()

# Create ports for Branchs and Branch stubs that connect to client stubs and other branch stubs.
        for branch_instance in branches:
            if branch_instance["id"] != self.id:
                port_number = branch_instance.get("port", 50050 + branch_instance["id"])
                channel_instance = grpc.insecure_channel(f'localhost:{port_number}')
                stub_instance = banks_pb2_grpc.BranchServiceStub(channel_instance)
                self.stubList.append(stub_instance)

        print(f"[ The Branch {self.id}] starts with balance_amount={self.balance}")

# Solo Method for every request
    def MsgDelivery(self, request, context):
        interface_instance = request.Interface_type
        if interface_instance == "query":
            return self.query_account()
        elif interface_instance == "deposit":
            return self.deposit_money(request.money)
        elif interface_instance == "withdraw":
            return self.withdraw_money(request.money)
        elif interface_instance == "propagate_deposit":
            return self.propagate_deposit(request.money)
        elif interface_instance == "propagate_withdraw":
            return self.propagate_withdraw(request.money)
        else:
            return banks_pb2.BranchResponse(
                id=self.id, Interface_type=interface_instance, result="fail", balance=self.balance
            )

    #Create functions for each interface that is delivered from the Branch to be returned to the client of the requesting customer ----
    def query_account(self):
        print(f"[Branch {self.id}] Query balance={self.balance}")
        return banks_pb2.BranchResponse(
            id=self.id, Interface_type="query", result="success", balance=self.balance
        )

    def deposit_money(self, money_amount):
        self.balance += money_amount
        print(f"[Branch {self.id}] Deposit +{money_amount}, new balance={self.balance}")
        self._propagate_interface_update(money_amount, "propagate_deposit")
        return banks_pb2.BranchResponse(
            id=self.id, Interface_type="deposit", result="success", balance=self.balance
        )

    #withdraw money from the branch balance and propagate the update to other branches.
    def withdraw_money(self, money_amount):
        if self.balance >= money_amount:
            self.balance -= money_amount
            print(f"[Branch {self.id}] Withdraw -{money_amount}, new balance={self.balance}")
            self._propagate_interface_update(money_amount, "propagate_withdraw")
            return banks_pb2.BranchResponse(
                id=self.id, Interface_type="withdraw", result="success", balance=self.balance
            )
        else:
            print(f"[Branch {self.id}] Withdraw failed (insufficient funds).")
            return banks_pb2.BranchResponse(
                id=self.id, Interface_type="withdraw", result="fail", balance=self.balance
            )

    def propagate_deposit(self, money_amount):
        self.balance += money_amount
        print(f"[Branch {self.id}] Propagated deposit +{money_amount}, balance={self.balance}")
        return banks_pb2.BranchResponse(
            id=self.id, Interface_type="propagate_deposit", result="success", balance=self.balance
        )

    def propagate_withdraw(self, money_amount):
        self.balance -= money_amount
        print(f"[Branch {self.id}] Propagated withdraw -{money_amount}, balance={self.balance}")
        return banks_pb2.BranchResponse(
            id=self.id, Interface_type="propagate_withdraw", result="success", balance=self.balance
        )

    #Update the balance of the Branch balance with the Branches request 
    def _propagate_interface_update(self, money_amount, method_name):
        for stub in self.stubList:
            try:
                rpc_message = getattr(stub, "MsgDelivery")  # always call MsgDelivery
                rpc_message(banks_pb2.BranchRequest(
                    id=self.id,
                    Interface_type=method_name,
                    money=money_amount
                ))
            except Exception as e:
                print(f"[Branch {self.id}] Propagation error: {e}")
