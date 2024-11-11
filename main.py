import datetime
import re

# This app takes a mon output and parses it to table insert for a data warehouse stats table

def generate_insert_statements(file_path):
    """
  Reads a file line by line, extracts data, and generates INSERT statements.

  Args:
    file_path: Path to the file containing the data.
  """

    insertPrepend = "INSERT INTO process_queue_wait_times (batch_queue_id, wait_milliseconds, source_table_name) VALUES"
    insert_statement = ""

    # Initialize variables to store extracted data
    data = {}
    current_table = None

    insideLastBatch = False
    insideIntegrationTime = False

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()

            #print(line)
            # Check for the start of a new table's data
            if line.startswith('│                                             │   {"') and line.endswith('│'):
                current_table = line.split('{"')[1].split('"')[0]
                data[current_table] = {}
                data[current_table]['source_table_name'] = current_table
            # Extract key-value pairs from the lines
            elif line.startswith('│                                             │') and line.endswith('│'):
                if current_table is not None and line.split(":") is not None:
                    try:
                        key, value = line.split(':', 1)  # Split only at the first colon
                    except ValueError:
                        #print(f"Warning: Skipping line without a colon: {line}")
                        if insideIntegrationTime:
                            insideIntegrationTime = False
                        elif insideLastBatch:
                            insideLastBatch = False
                        continue  # Move to the next line

                    key = key.split('"')[1].strip().replace(" ", "_").replace('-', '_').lower()
                    value = value.strip().replace(',', '').replace("│", "").replace('"', "").strip()

                    # Add the "last_batch_" prefix if the key is within the "Last batch info" block
                    if "last_batch_info" in key:
                        insideLastBatch = True
                    if "integration_task_time" in key:
                        insideIntegrationTime = True

                    if insideLastBatch:
                        key = "last_batch_" + key

                    if value != "{":
                        try:
                            # Convert numeric values to integers
                            value = int(value)
                        except ValueError:
                            pass
                        try:
                            # Convert datetime to datetime
                            value = datetime.datetime.fromisoformat(str(value))
                        except ValueError:
                            pass
                        data[current_table][key] = value

    print("TRUNCATE TABLE process_queue_wait_times;")

    # Generate INSERT statements for each table
    insert_statements = []
    for table_name, table_data in data.items():
        columns = ', '.join(table_data.keys())
        values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in table_data.values()])
        insert_statement = f"INSERT INTO public.process_queue_wait_times ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)

    # Print the INSERT statements
    for insert_statement in insert_statements:
        print(insert_statement)


# Example usage:
file_path = '/Users/danielferrara/Downloads/mon_kswic_target2.txt'
generate_insert_statements(file_path)
