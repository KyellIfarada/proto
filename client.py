import json
import sys
import time
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
        events = customerInfo.get("events", [])
        customer_instance = Customer(c_id, events)

        # Map customer ID to branch ID if customer id exists in branch IDs with port number otherwise create a default port for the branch with its own ID.
        if c_id in branch_ids_database:
            branch_address = f"localhost:{50050 + c_id}"
        else:
            branch_address = f"localhost:{50050 + branch_ids_database[0]}"

        # Create gRPC stub of the Branch for the customer to connect to the branch
        try:
            customer_instance.createStub(branch_address)
        except Exception as z:
            print(f"[Client] Failure to produce stub pertaining to Customer {c_id} at branch address {branch_address}: {z}")

        # Add Customer objects to list of customers.
        customersObjList.append(customer_instance)


    print("[Client] Intiating performance of customer events...")

    # Perform events in order of each customer
    for customer_instance in customersObjList:
        output_events = []
        for event_instance in customer_instance.events:
            try:
                balance_result = customer_instance.executeSingleEvent(event_instance)
                
                # Only include "balance" for query, "result" for deposit/withdraw
                if event_instance["interface"] == "query":
                    output_events.append({"interface": "query", "balance": balance_result['balance']})
                else:
                    output_events.append({"interface": event_instance["interface"], "result": "success"})
            except Exception as z:
                print(f"[Client] Error executing event {event_instance} for Customer {customer_instance.id}: {z}")

            time.sleep(1)  # small delay for branch propagation

        customer_instance.recvMsg = output_events

    # Save output JSON
    output = [{"id": a.id, "recv": a.recvMsg} for a in customersObjList]
    output_file = "output.json"
    try:
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"[Client] Output successfully written to {output_file}")
    except Exception as b:
        print(f"[Client] Failed to write output file: {b}")

if __name__ == "__main__":
    main()
