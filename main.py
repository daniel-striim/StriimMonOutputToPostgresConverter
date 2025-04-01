import datetime

# This app takes a mon output and parses it to table insert for a data warehouse stats table

def generate_insert_statements(file_path):
    """
  Reads a file line by line, extracts data, and generates INSERT statements.

  Args:
    file_path: Path to the file containing the data.
  """

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
            if line.startswith('│                                             │   "') and line.endswith('│'):
                current_table = line.split('"')[1]
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

    # Define the set of valid columns based on your DDL
    valid_columns = { 'batch_queue_id', 'wait_milliseconds', 'source_table_name', 'total_batches_created', 'partition_pruned_batches', 'last_successful_merge_time', 'total_batches_ignored', 'max_integration_time_in_ms', 'avg_in_mem_compaction_time_in_ms', 'avg_batch_size_in_bytes', 'no_of_updates', 'no_of_inserts', 'total_events_merged', 'no_of_ddls', 'no_of_deletes', 'no_of_pkupdates', 'avg_event_count_per_batch', 'min_integration_time_in_ms', 'mapped_source_table', 'total_batches_queued',
                      'avg_compaction_time_in_ms', 'avg_waiting_time_in_queue_in_ms', 'avg_integration_time_in_ms', 'total_batches_uploaded', 'avg_merge_time_in_ms', 'last_batch_no_of_updates', 'last_batch_event_count', 'last_batch_no_of_inserts', 'last_batch_max_record_size', 'last_batch_total_events_merged', 'last_batch_no_of_ddls', 'last_batch_sequence_number', 'last_batch_size_in_bytes', 'last_batch_compaction_time_in_ms', 'last_batch_stage_resources_management_time_in_ms', 'last_batch_upload_time_in_ms', 'last_batch_merge_time_in_ms', 'last_batch_in_memory_compaction_time_in_ms', 'last_batch_pk_update_time_in_ms', 'last_batch_ddl_execution_time_in_ms', 'last_batch_total_integration_time_in_ms', 'last_batch_no_of_deletes', 'last_batch_no_of_pkupdates', 'last_batch_accumulation_time_in_ms', 'avg_stage_resources_management_time_in_ms', 'avg_upload_time_in_ms', 'last_batch_batch_event_count', 'last_batch_max_record_size_in_batch', 'last_batch_batch_sequence_number', 'last_batch_batch_size_in_bytes', 'last_batch_batch_accumulation_time_in_ms' }

    # Generate INSERT statements for each table
    insert_statements = []
    for table_name, table_data in data.items():
        filtered_data = {k: v for k, v in table_data.items() if k in valid_columns}

        columns = ', '.join(filtered_data.keys())
        values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in filtered_data.values()])
        insert_statement = f"INSERT INTO public.process_queue_wait_times ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)

    # Print the INSERT statements
    for insert_statement in insert_statements:
        print(insert_statement)


# usage:
file_path = '/Users/danielferrara/Downloads/mon_integra_cdc.txt'
generate_insert_statements(file_path)
