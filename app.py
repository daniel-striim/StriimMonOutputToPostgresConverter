import datetime
import io
import pandas as pd
from flask import Flask, render_template, request, session, redirect, url_for, Response, flash
import secrets
import uuid
from cachelib import SimpleCache
import traceback # For printing full tracebacks

app = Flask(__name__)

# --- Configuration ---
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 # 50 Megabytes
app.secret_key = secrets.token_hex(16)

# --- Cache Setup ---
cache = SimpleCache()
CACHE_TIMEOUT = 3600 # 1 hour

# --- VALID_COLUMNS definition (ensure it matches original script's intent) ---
VALID_COLUMNS = {
    # Copied directly from original script for consistency
    'batch_queue_id', 'wait_milliseconds', 'source_table_name', 'total_batches_created',
    'partition_pruned_batches', 'last_successful_merge_time', 'total_batches_ignored',
    'max_integration_time_in_ms', 'avg_in_mem_compaction_time_in_ms',
    'avg_batch_size_in_bytes', 'no_of_updates', 'no_of_inserts', 'total_events_merged',
    'no_of_ddls', 'no_of_deletes', 'no_of_pkupdates', 'avg_event_count_per_batch',
    'min_integration_time_in_ms', 'mapped_source_table', 'total_batches_queued',
    'avg_compaction_time_in_ms', 'avg_waiting_time_in_queue_in_ms',
    'avg_integration_time_in_ms', 'total_batches_uploaded', 'avg_merge_time_in_ms',
    # Note: Original script prefixes keys *within* the loop, so VALID_COLUMNS
    # should contain the *prefixed* keys expected from last_batch_info block
    'last_batch_no_of_updates', 'last_batch_event_count', 'last_batch_no_of_inserts',
    'last_batch_max_record_size', 'last_batch_total_events_merged', 'last_batch_no_of_ddls',
    'last_batch_sequence_number', 'last_batch_size_in_bytes', 'last_batch_compaction_time_in_ms',
    'last_batch_stage_resources_management_time_in_ms', 'last_batch_upload_time_in_ms',
    'last_batch_merge_time_in_ms', 'last_batch_in_memory_compaction_time_in_ms',
    'last_batch_pk_update_time_in_ms', 'last_batch_ddl_execution_time_in_ms',
    'last_batch_total_integration_time_in_ms', 'last_batch_no_of_deletes',
    'last_batch_no_of_pkupdates', 'last_batch_accumulation_time_in_ms',
    # Add other possible prefixed keys if the original logic could generate them
    'last_batch_avg_stage_resources_management_time_in_ms', # From previous flask attempt
    'last_batch_avg_upload_time_in_ms', # From previous flask attempt
    'last_batch_batch_event_count', # From previous flask attempt
    'last_batch_max_record_size_in_batch',# From previous flask attempt
    'last_batch_batch_sequence_number',# From previous flask attempt
    'last_batch_batch_size_in_bytes',# From previous flask attempt
    'last_batch_batch_accumulation_time_in_ms', # From previous flask attempt
    'last_batch_avg_compaction_time_in_ms', # From previous flask attempt
    'last_batch_avg_merge_time_in_ms', # From previous flask attempt
    # IMPORTANT: Also include the marker keys if the original logic stored them
    'last_batch_last_batch_info',
    'last_batch_integration_task_time'
}


# --- Parser Function adjusted to closely match original logic ---
def parse_log_stream(log_stream):
    """
    Parses log data line-by-line from stream, closely matching original script's logic.
    Returns a Pandas DataFrame.
    """
    data_list = []
    current_table_data = None
    # Flags managed similar to original script
    insideLastBatch = False
    insideIntegrationTime = False # Note: Original script didn't seem to use this flag effectively after setting
    line_number = 0
    decode_errors = 0

    print("DEBUG: Starting stream parsing (Revised Logic)...")

    try:
        for line_bytes in log_stream:
            line_number += 1
            try:
                # Decode with fallback (same as before)
                line = line_bytes.decode('utf-8').strip()
            except UnicodeDecodeError:
                try:
                    line = line_bytes.decode('latin-1').strip()
                except UnicodeDecodeError:
                    if decode_errors < 5: print(f"Warning: Skipping line {line_number} due to undecodable bytes.")
                    decode_errors += 1; continue

            if not line: continue

            # --- Use original script's startswith logic ---
            # IMPORTANT: Replace the literal string here with the exact one from your ORIGINAL
            # working script if it differs subtly (e.g., number of spaces)
            # Using the one provided in the prompt:
            original_header_start = '│                                             │   "'
            original_kv_start = '│                                             │'

            # Check for the start of a new table's data (matching original)
            # Ensure we don't match the kv_start by checking for the extra '   "'
            if line.startswith(original_header_start) and line.endswith('│'):
                print(f"DEBUG: Line {line_number}: Matched Header") # DEBUG
                # If we were processing a previous table, add its data to the list
                if current_table_data:
                    data_list.append(current_table_data)

                # Start new table data
                try:
                    current_table_name = line.split('"')[1]
                    current_table_data = {'source_table_name': current_table_name}
                    # Reset flags for the new table
                    insideLastBatch = False
                    insideIntegrationTime = False # Resetting here seems logical, though original didn't explicitly
                except IndexError:
                    print(f"Warning: Line {line_number}: Could not extract table name from header: {line}")
                    current_table_data = None # Invalidate current table if header parse fails
                continue # Move to next line after processing header

            # Extract key-value pairs from the lines (matching original)
            elif line.startswith(original_kv_start) and line.endswith('│') and current_table_data is not None:
                 # Check if it's potentially a header line misidentified (contains '"' near start)
                if '   "' in line[len(original_kv_start):len(original_kv_start)+5]: # Basic check
                     print(f"DEBUG: Line {line_number}: Skipped KV match, looks like header: {line}") # DEBUG
                     continue

                print(f"DEBUG: Line {line_number}: Matched Key-Value candidate") # DEBUG

                # Original script's check `line.split(":") is not None` is always true if split succeeds
                # The core check is the try/except around the split itself.
                try:
                    key_part, value_part = line.split(':', 1)  # Split only at the first colon
                except ValueError:
                    # Original script's handling of lines without a colon
                    print(f"DEBUG: Line {line_number}: Skipping line without colon (ValueError): {line}") # DEBUG
                    if insideIntegrationTime: # Original reset logic
                        insideIntegrationTime = False
                    elif insideLastBatch:
                        insideLastBatch = False
                    continue  # Move to the next line

                # --- Process Key (matching original) ---
                try:
                    key = key_part.split('"')[1].strip().replace(" ", "_").replace('-', '_').lower()
                except IndexError:
                     print(f"Warning: Line {line_number}: Could not extract key from: {key_part}")
                     continue # Skip if key extraction fails

                # --- Process Value (matching original) ---
                value = value_part.strip().replace(',', '').replace("│", "").replace('"', "").strip()

                # --- State Management & Prefixing (matching original) ---
                # Check flags *before* prefixing, based on the *unprefixed* key
                is_last_batch_marker = "last_batch_info" == key # Use exact key match
                is_integration_time_marker = "integration_task_time" == key # Use exact key match

                if is_last_batch_marker:
                    insideLastBatch = True
                    # Original script didn't reset insideIntegrationTime here, seems correct?

                if is_integration_time_marker:
                     # Original script just set this flag, didn't seem conditional on insideLastBatch
                     insideIntegrationTime = True

                # Apply prefix *if* inside the block (original logic)
                if insideLastBatch:
                     # Add prefix, including to the marker keys themselves if stored
                     key_to_store = "last_batch_" + key
                else:
                     key_to_store = key

                # --- Value Conversion & Storage (matching original) ---
                # Original script stored the marker keys with '{', let's skip storing '{' explicitly
                if value != "{":
                    processed_value = value # Start with the cleaned string value
                    try:
                        # 1. Try converting to integer (original order)
                        processed_value = int(value)
                    except ValueError:
                        try:
                            # 2. Try converting to datetime (original order)
                            # Ensure value is a string for fromisoformat
                            dt_value_str = str(value)
                            if dt_value_str.endswith('Z'): dt_value_str = dt_value_str[:-1] + '+00:00'
                            processed_value = datetime.datetime.fromisoformat(dt_value_str)
                        except (ValueError, TypeError):
                            # If both int and datetime fail, keep the cleaned string
                            pass # processed_value remains the string 'value'

                    # Store the processed key and value
                    current_table_data[key_to_store] = processed_value
                    print(f"DEBUG: Line {line_number}: Stored key='{key_to_store}', value='{processed_value}' (type: {type(processed_value)})") # DEBUG
                else:
                     # Handle the '{' value - original stored it, maybe we should too?
                     # Or just use it as a marker and don't store? Let's skip storing '{'.
                     print(f"DEBUG: Line {line_number}: Encountered '{{' for key='{key_to_store}', skipping storage of value.") # DEBUG
                     # Optionally store a placeholder if needed: current_table_data[key_to_store] = None

            # --- End of core parsing logic ---

    except Exception as e:
        print(f"ERROR: Exception during stream parsing near line {line_number}: {e}")
        traceback.print_exc()
        raise e # Re-raise to be caught by route handler
    finally:
        # Add the very last table's data if it exists
        if current_table_data:
            print("DEBUG: Appending last table data.") # DEBUG
            data_list.append(current_table_data)
        print(f"DEBUG: Finished stream parsing. Total lines read: {line_number}, Decode errors: {decode_errors}")

    if not data_list:
        print("DEBUG: No data extracted into list.")
        return pd.DataFrame()

    df = pd.DataFrame(data_list)
    print(f"DEBUG: Created DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")

    # --- Type Conversion after DataFrame creation (keep previous robust logic) ---
    # (This part can remain as it was, it's generally good practice)
    for col in df.columns:
         if df[col].dtype == 'object':
            try:
                converted_dt = pd.to_datetime(df[col], errors='coerce')
                if not converted_dt.isna().all():
                    df[col] = converted_dt
                else:
                     converted_num = pd.to_numeric(df[col], errors='coerce')
                     if not converted_num.isna().all():
                          # Check if converting to int makes sense (no decimals)
                          if (converted_num == converted_num.round()).all():
                              df[col] = converted_num.astype(pd.Int64Dtype()) # Use nullable Int
                          else:
                               df[col] = converted_num # Keep as float
            except Exception as conversion_err:
                 print(f"DEBUG: Error converting column {col} type post-creation: {conversion_err}")
                 pass

    print(f"DEBUG: DataFrame final dtypes:\n{df.dtypes}") # DEBUG
    return df


# --- SQL Generation Function (REVISED to match original script's logic) ---
def generate_sql_inserts(df):
    """
    Generates PostgreSQL INSERT statements from a Pandas DataFrame,
    mimicking the original script's formatting logic BUT skipping rows
    where 'total_batches_created' is NaN/None.
    WARNING: Still uses original script's potentially unsafe value formatting.
    """
    if df is None or df.empty:
        return "-- No data to generate INSERT statements for."

    # TRUNCATE statement first
    insert_statements = ["TRUNCATE TABLE public.process_queue_wait_times;"]

    valid_columns = VALID_COLUMNS
    valid_df_columns = [col for col in df.columns if col in valid_columns]

    if not valid_df_columns:
         print(f"WARN: No columns in DataFrame matched VALID_COLUMNS. DataFrame columns: {df.columns.tolist()}") # DEBUG
         return "-- No valid columns found in the parsed data matching VALID_COLUMNS."

    filtered_df = df[valid_df_columns]
    print(f"DEBUG: Columns considered for SQL generation: {valid_df_columns}") # DEBUG

    # Define the specific column to check for NaN
    nan_check_column = 'total_batches_created'
    rows_skipped = 0

    # Iterate through each row of the filtered DataFrame
    for index, row in filtered_df.iterrows():

        # --- ADDED: Check for NaN in the specific column ---
        # Check if the column exists in this row's index (it should) and if the value is NaN/None
        if nan_check_column in row.index and pd.isna(row[nan_check_column]):
            rows_skipped += 1
            print(f"DEBUG: Skipping SQL for row index {index} because '{nan_check_column}' is NaN/None.") # DEBUG
            continue # Skip the rest of the loop for this row
        # --- End NaN Check ---

        # --- If the row is kept, proceed with mimicking Original Script's Logic ---
        row_data = row # Use the full row Series
        columns = ', '.join(row_data.index) # No quotes on columns
        values_list = []
        for value in row_data.values:
            if isinstance(value, str):
                # Original logic: No quote escaping
                values_list.append(f"'{value}'")
            else:
                # Original logic: Use str() for everything else (None->'None', NaN->'nan', etc.)
                values_list.append(str(value))

        values = ', '.join(values_list)
        insert_statement = f"INSERT INTO public.process_queue_wait_times ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)
        # --- End Mimic Original Logic ---F

    if rows_skipped > 0:
        print(f"DEBUG: Skipped generating SQL for {rows_skipped} rows due to NaN in '{nan_check_column}'.")
        # Optionally add a comment to the SQL output
        insert_statements.append(f"-- Note: Skipped {rows_skipped} rows where '{nan_check_column}' was missing.")

    return "\n".join(insert_statements)

# --- Flask Routes (No changes needed from previous) ---
@app.route('/', methods=['GET'])
def index():
    old_key = session.pop('parsed_data_key', None)
    if old_key: cache.delete(old_key)
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_logs():
    log_stream = None; source_description = ""
    if 'log_file' in request.files and request.files['log_file'].filename != '':
        file = request.files['log_file']; print(f"DEBUG: Processing uploaded file: {file.filename}")
        source_description = f"uploaded file '{file.filename}'"; log_stream = file.stream
        if not hasattr(log_stream, 'read'):
             flash(f"Error: Could not get a readable stream from uploaded file '{file.filename}'.", "error")
             print(f"ERROR: Uploaded file object for {file.filename} lacks read method.")
             return redirect(url_for('index'))
    elif 'log_text' in request.form and request.form['log_text'].strip():
        log_content = request.form['log_text']; print("DEBUG: Processing pasted text.")
        source_description = "pasted text"; log_stream = io.BytesIO(log_content.encode('utf-8'))
    else:
        flash("No input provided. Please paste log content or upload a file.", "warning")
        print("DEBUG: No valid file or text input detected.")
        return redirect(url_for('index'))
    if log_stream:
        try:
            print(f"DEBUG: Calling parse_log_stream for {source_description}")
            parsed_df = parse_log_stream(log_stream)
            print(f"DEBUG: Parsing returned DataFrame with {len(parsed_df)} rows.")
            if parsed_df.empty:
                 flash(f"Parsing completed, but no valid table data was found in {source_description}. Please check the log format.", "warning")
                 print(f"DEBUG: Parsing successful but resulted in empty DataFrame for {source_description}")
                 return redirect(url_for('index'))
            cache_key = str(uuid.uuid4())
            cache.set(cache_key, parsed_df, timeout=CACHE_TIMEOUT)
            session['parsed_data_key'] = cache_key
            print(f"DEBUG: Stored DataFrame in cache with key {cache_key}")
            flash(f"Successfully parsed {source_description}. Found {len(parsed_df)} records.", "success")
            return redirect(url_for('results'))
        except Exception as e:
            flash(f"An error occurred while processing {source_description}. Please check logs for details.", "error")
            print(f"ERROR: Exception during processing of {source_description}: {e}")
            traceback.print_exc()
            return redirect(url_for('index'))
        finally:
             if hasattr(log_stream, 'close') and callable(log_stream.close):
                  try: log_stream.close(); print("DEBUG: Input stream closed.")
                  except Exception as close_err: print(f"Warning: Could not close stream: {close_err}")
    else:
        flash("Failed to obtain input stream.", "error"); return redirect(url_for('index'))

@app.route('/results', methods=['GET'])
def results():
    """
    Retrieves data from cache, filters it, and prepares data for display
    with specified default visible columns.
    """
    cache_key = session.get('parsed_data_key')
    if not cache_key:
        flash("No parsed data key found in session. Please submit logs first.", "info")
        return redirect(url_for('index'))

    try:
        df = cache.get(cache_key)
        if df is None:
            flash("Parsed data has expired or was not found in cache. Please submit logs again.", "warning")
            session.pop('parsed_data_key', None)
            return redirect(url_for('index'))

        print(f"DEBUG: DataFrame shape before filtering: {df.shape}")

        # --- Filtering Step (remains the same) ---
        filter_column = 'total_batches_created'
        if filter_column in df.columns:
            original_rows = len(df)
            rows_to_keep = df[filter_column].notna()
            df_filtered = df[rows_to_keep].copy()
            filtered_rows = len(df_filtered)
            rows_removed = original_rows - filtered_rows
            print(f"DEBUG: Filtering on '{filter_column}'. Removed {rows_removed} rows with missing values.")
            # Don't flash here, maybe flash only if all removed?
            # if rows_removed > 0:
            #     flash(f"Filtered out {rows_removed} records with missing '{filter_column}'.", "info")
            df = df_filtered
        else:
            print(f"DEBUG: Filter column '{filter_column}' not found, skipping filter.")
            # flash(f"Warning: Column '{filter_column}' not found, cannot filter by it.", "warning")
        # --- End Filtering Step ---

        print(f"DEBUG: DataFrame shape after filtering: {df.shape}")

        # Define the columns that should be VISIBLE BY DEFAULT
        # ** IMPORTANT: Verify these names EXACTLY match columns in your DataFrame **
        # Check the DEBUG log output for df.columns if unsure.
        default_visible_columns = [
            'source_table_name',         # Assuming this maps to user's 'sourceName'
            # 'mapped_source_table',       # Assuming this maps to user's 'targetName' (or maybe it doesn't exist?)
            'last_successful_merge_time',
            'max_integration_time_in_ms', # Verify suffix '_in_ms' is correct
            'avg_batch_size_in_bytes',    # Verify suffix '_in_bytes' is correct
            'avg_event_count_per_batch',
            'total_batches_queued'
        ]

        if df.empty:
             flash("No data remaining after filtering.", "warning")
             header_info = []
             data_for_table = []
             actual_default_columns = [] # Pass empty list if no data
        else:
             # Check which requested default columns actually exist in the df
             actual_default_columns = [col for col in default_visible_columns if col in df.columns]
             if len(actual_default_columns) < len(default_visible_columns):
                  missing_defaults = set(default_visible_columns) - set(actual_default_columns)
                  print(f"WARN: Some requested default columns not found in DataFrame: {missing_defaults}")
                  flash(f"Warning: Some default columns were not found: {', '.join(missing_defaults)}", "warning")

             # Prepare header info (needed for dropdowns and potentially colvis text)
             header_info = [{'name': header, 'index': i} for i, header in enumerate(df.columns)]
             # Prepare data for table body
             data_for_table = df.applymap(lambda x: None if pd.isna(x) else x).values.tolist()

        # Pass header_info, data, and the list of *actual* default column names
        return render_template('results.html',
                               header_info=header_info,
                               data=data_for_table,
                               default_columns=actual_default_columns) # Pass the validated list

    except Exception as e:
        flash(f"Error loading results: {e}", "error")
        print("ERROR loading results:")
        traceback.print_exc()
        return redirect(url_for('index'))


@app.route('/get_sql', methods=['GET'])
def get_sql():
    cache_key = session.get('parsed_data_key')
    if not cache_key: return Response("-- No parsed data key found in session.", mimetype='text/plain', status=404)
    try:
        df = cache.get(cache_key)
        if df is None: return Response("-- Parsed data has expired or was not found in cache.", mimetype='text/plain', status=404)
        sql_statements = generate_sql_inserts(df)
        return Response(sql_statements, mimetype='text/plain')
    except Exception as e:
        print(f"Error generating SQL: {e}"); traceback.print_exc(); return Response(f"-- Error generating SQL: {e}", mimetype='text/plain', status=500)

@app.errorhandler(413)
def request_entity_too_large(error):
    flash("The pasted text or uploaded file was too large for the server to accept directly. Please use the file upload option for large logs, or check server configuration.", "error")
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)