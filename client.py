import json
import sys
import time
from customer import Customer

def main():

    # Load File to read of customer and branch info
    load_file = sys.argv[1]

    # Load input file
    try:
        with open(load_file, "r") as file:
            database = json.load(file)
    except Exception as b:
        print(f"[Client] Failure to read input file for processing: {b}")
        return

    # Divide customers and branches entries into two separate data sets
    customers_database = [entry for entry in database if entry["type"] == "customer"]
    branches_database = [entry for entry in database if entry["type"] == "branch"]

    # Create a list to hold all customer objects 
    customersObjList = []

    # Create Customer objects 
    for customerInfo in customers_database:
        c_id = customerInfo["id"]
        events = customerInfo.get("events", [])
        customer_instance = Customer(c_id, events)

        # Map branch IDs to ports
        branch_list = [{"id": branch["id"], "port": 50050 + branch["id"]} 
                       for branch in branches_database]

        # Create gRPC stubs 
        try:
            customer_instance.createStub(branch_list)
        except Exception as z:
            print(f"[Client] Failure to produce stub for Customer {c_id}: {z}")

        customersObjList.append(customer_instance)

    print("[Client] Intiating performance of customer events...")

    # Process events and add results to each customer's recvMsg
    for customer_instance in customersObjList:
        output_events = []

        for event_instance in customer_instance.events:
            try:
                response = customer_instance.executeSingleEvent(event_instance)

                if event_instance["interface"] == "query":
                    output_events.append({
                        "interface": "query",
                        "branch": response["id"],
                        "balance": response["balance"]
                    })
                else:
                    output_events.append({
                        "interface": event_instance["interface"],
                        "branch": response["id"],
                        "result": response["result"]
                    })

            except Exception as z:
                print(f"[Client] Error performing event {event_instance} by Customer {customer_instance.id}: {z}")

            time.sleep(1)  # leave propagation time for withdraw and deposit events

        customer_instance.recvMsg = output_events

    # Save output JSON
    output = [{"id": c.id, "recv": c.recvMsg} for c in customersObjList]

    try:
        with open("output.json", "w") as f:
            json.dump(output, f, indent=2)
        print("[Client] Output correctly put to output.json")
    except Exception as b:
        print(f"[Client] Unsuccesful write to outgoing file: {b}")

if __name__ == "__main__":
    main()
