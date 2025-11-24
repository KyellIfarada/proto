import json
import sys
import time
import grpc 
import banks_pb2_grpc, banks_pb2
from customer import Customer

def main():
    
    #Load File to read of customer and branch info
    load_file = sys.argv[1]

    # Load input file
    try:
        with open(load_file, "r") as file:
            database = json.load(file)
    except Exception as b:
        print(f"[Client] Failure to read input file for processing: {b}")
        return

    # Divide customers and branches entries into two seperate data sets ( lists ) and gather branch IDs
    customers_database = [Interface_type for Interface_type in database if Interface_type["type"] == "customer"]
    branches_database = [Interface_type for Interface_type in database if Interface_type["type"] == "branch"]
    branch_ids_database = [branch["id"] for branch in branches_database]

    # Create a list to hold all customer objects 
    customersObjList = []

    # Create Customer objects and their attributes 
    for customerInfo in customers_database:
        c_id = customerInfo["id"]

        events_nformatted = customerInfo["customer-requests"]

        events_formatted = []
        for event in events_nformatted:
            events_formatted.append({
                "id": event["customer-request-id"],
                "interface": event["interface"],
                "money": event.get("money", 0)
            })

        customer_instance = Customer(c_id, events_formatted)
        customer_instance.logical_clock = customerInfo["id"]

        # Map customer ID to branch ID if customer id exists in branch IDs with port number otherwise create a default port for the branch with its own ID.
        if c_id in branch_ids_database:
            branch_address = f"localhost:{50050 + c_id}"
        else:
            branch_address = f"localhost:{50050 + branch_ids_database[0]}"

        
        # Create gRPC stub of the Branch for the customer to connect to the branch
        try:
            customer_instance.createStub(branch_address)
        except Exception as a:
            print(f"[Client] Failure to produce stub pertaining to Customer {c_id} at branch address {branch_address}: {a}")

        # Add Customer objects to list of customers.
        customersObjList.append(customer_instance)

    print("[Client] Intiating performance of customer events...")

    # Perform events in order of each customer
    for customer_instance in customersObjList:
        for event_instance in customer_instance.events:
            try:
                balance_result = customer_instance.executeSingleEvent(event_instance)
                time.sleep(1.0)  # delay for branch propagation
            except Exception as b:
                print(f"[Client] Error executing event {event_instance} for Customer {customer_instance.id}: {b}")

    # following customer events, request logs from all branches
    branches_result = []
    complete_request_events = []
    
    for branch in branches_database:
        branch_id = branch["id"]
        branch_port = branch.get("port", 50050 + branch_id)
        # Build a temporary customer stub to fetch logs - or use gRPC channel

        try:

            channel = grpc.insecure_channel(f'localhost:{branch_port}')
            stub = banks_pb2_grpc.BranchServiceStub(channel)

            # Request get_log 
            log_request = banks_pb2.BranchRequest(
                id=0, 
                Interface_type="get_log",  # top-level field in BranchRequest
                money=0, 
                customer_request_id=0, 
                logicalClock=0
            )  
            
            log_response = stub.MsgDelivery(log_request)

            branch_events = []
            for a in log_response.events_log:  # <-- corrected field name
                
                branch_events.append({
                    "id": a.id,
                    "customer-request-id": a.customer_request_id,
                    "logical-clock": a.logicalClock,
                    "interface": a.interface_type,   # <-- corrected field name
                    "comment": a.comment
                })

                # add to the global request-events list, attach branch id
                complete_request_events.append({
                    "id": a.id, 
                    "customer-request-id": a.customer_request_id,
                    "type": "branch",
                    "logical-clock": a.logicalClock,
                    "interface": a.interface_type,   # <-- corrected field name
                    "comment" : a.comment
                })

            branches_result.append({
                "id": branch_id,
                "type": "branch",
                "events": sorted(branch_events, key=lambda b: b["logical-clock"])
            })

        except Exception as a:
            print(f"[Client] Error fetching logs for Branch {branch_id}: {a}")


    customer_output_list = []

    for customer_instance in customersObjList:
        customer_output_list.append({
            "id": customer_instance.id,
            "type": "customer",
            "events": customer_instance.recvMsg
        })

    # add to global request list
    for a in customer_instance.recvMsg:
        complete_request_events.append({
            "id": customer_instance.id,
            "customer-request-id": a["customer-request-id"],
            "type": "customer",
            "logical-clock": a["logical-clock"],
            "interface": a["interface"],
            "comment": a["comment"]
        })


    # Save output JSON
    output = { 
        "customers": customer_output_list,
        "branches": branches_result,
        "request-events": sorted(complete_request_events, key=lambda c: c["logical-clock"])
    }

    output_file = "output.json"
    try:
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"[Client] Output successfully written to {output_file}")
    except Exception as b:
        print(f"[Client] Failed to write output file: {b}")

if __name__ == "__main__":
    main()
