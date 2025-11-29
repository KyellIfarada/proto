import grpc
import banks_pb2, banks_pb2_grpc
import time
import threading

class Branch(banks_pb2_grpc.BranchServiceServicer):

    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = []
        self.recvMsg = []
        self.lock = threading.Lock()

        self.set_of_writes = set()
        self.write_order = 0

        for branch_instance in branches:
            port_number = branch_instance.get("port", 50050 + branch_instance["id"])
            channel = grpc.insecure_channel(f"localhost:{port_number}")
            stub = banks_pb2_grpc.BranchServiceStub(channel)
            self.stubList.append(stub)

        print(f"[[Branch] {self.id}] starts with balance_amount={self.balance}")

    def MsgDelivery(self, request, context):

        client_side_set_of_writes = set(int(b) for b in request.set_of_writes)

        interface_instance = request.interface_type

        # Read Your Writes for queries
        if interface_instance == "query":
            # error handling for missing writes from request.set_of_writes
            while not client_side_set_of_writes.issubset(self.set_of_writes):
                time.sleep(0.02)
            return self.query_account()

        elif interface_instance == "deposit":
            while not client_side_set_of_writes.issubset(self.set_of_writes):
                time.sleep(0.02)
            return self.deposit_money(request.money)

        elif interface_instance == "withdraw":
            while not client_side_set_of_writes.issubset(self.set_of_writes):
                time.sleep(0.02)
            return self.withdraw_money(request.money)
        
        #establish write id for propagation methods

        elif interface_instance == "propagate_deposit":
            if request.write_id:
                with self.lock:
                    self.set_of_writes.add(int(request.write_id))
            return self.propagate_deposit(request.money)
        
        #establish write id for propagation methods

        elif interface_instance == "propagate_withdraw":
            if request.write_id:
                with self.lock:
                 self.set_of_writes.add(int(request.write_id))
            return self.propagate_withdraw(request.money)

        return banks_pb2.BranchResponse(
            id=self.id, interface_type=interface_instance,
            result="fail", balance=self.balance
        )

    # Query current account balance
    def query_account(self):
        print(f"[[Branch] {self.id}] Query balance={self.balance}")
        return banks_pb2.BranchResponse(
            id=self.id, interface_type="query", result="success", balance=self.balance
        )

    # Deposit money into account and asynchronously propagate to other branches
    def deposit_money(self, money_amount):

        self.write_order += 1
        write_id = self.id * 1000 + self.write_order 
        self.balance += money_amount
        self.set_of_writes.add(write_id)

        print(f"[[Branch] {self.id}] Deposit +{money_amount}, new balance={self.balance}")
        
        threading.Thread(
        target=self._propagate_interface_update, args=(money_amount, "propagate_deposit", write_id)
        ).start()


        return banks_pb2.BranchResponse(
            id=self.id, interface_type="deposit", result="success",
            balance=self.balance, write_id=write_id
        )

    # Withdraw money from account and asynchronously propagate to other branches
    def withdraw_money(self, money_amount):

        if self.balance < money_amount:
            print(f"[[Branch] {self.id}] Withdraw failed.")
            return banks_pb2.BranchResponse(
                id=self.id, interface_type="withdraw", result="fail", balance=self.balance
            )

        self.write_order += 1
        write_id = self.id * 1000 + self.write_order 

        self.balance -= money_amount
        self.set_of_writes.add(write_id)

        print(f"[[Branch] {self.id}] Withdraw -{money_amount}, new balance={self.balance}")

        threading.Thread(
            target=self._propagate_interface_update, args=(money_amount, "propagate_withdraw", write_id)
        ).start()

        return banks_pb2.BranchResponse(
            id=self.id, interface_type="withdraw", result="success",
            balance=self.balance, write_id=write_id
        )

    def propagate_deposit(self, money_amount):
        with self.lock:
            self.balance += money_amount
            print(f"[[Branch] {self.id}] Propagated deposit +{money_amount}")
            return banks_pb2.BranchResponse(
                id=self.id, interface_type="propagate_deposit", result="success", balance=self.balance
        )

    def propagate_withdraw(self, money_amount):
        with self.lock:
            self.balance -= money_amount
            print(f"[[Branch] {self.id}] Propagated withdraw -{money_amount}")
        return banks_pb2.BranchResponse(
            id=self.id, interface_type="propagate_withdraw", result="success", balance=self.balance
        )

    def _propagate_interface_update(self, money_amount, method_name, write_id=None):
        for branch_instance in self.branches:

            # no self propagation
            if branch_instance["id"] == self.id:
                continue  

            port = branch_instance["port"]
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = banks_pb2_grpc.BranchServiceStub(channel)

            try:
                request = banks_pb2.BranchRequest(
                    id=self.id,
                    interface_type=method_name,
                    money=money_amount,
                    write_id=write_id,
                    
                )
                stub.MsgDelivery(request)
            except Exception as e:
                print(f"[[Branch] {self.id}] Propagation fail: {e}")
