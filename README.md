# Log Parser Web Application

## Overview

This Flask web application provides a user interface for parsing specific log files (or pasted log content). It extracts structured data from the logs, displays it in an interactive table, and allows the user to generate PostgreSQL `INSERT` statements based on the parsed data.

The application is designed to handle potentially large log inputs through file streaming and uses server-side caching for parsed data during a user session. It replaces an earlier standalone Python script, aiming to replicate its core parsing and SQL generation logic within a web framework, while adding UI enhancements.

## Features

* **Web Interface:** Provides an easy-to-use web UI built with Flask.
* **Flexible Input:** Accepts log data either by pasting text directly into a textarea or by uploading a log file.
* **Log Parsing:** Parses a specific log format line-by-line to extract key-value data associated with table names. Handles nested structures like "Last batch info".
* **Large File Handling:** Processes input streams line-by-line and uses server-side caching (`cachelib`) to manage parsed data, avoiding browser/session limitations for large inputs. Configurable request size limit via Flask settings.
* **Data Filtering:** Automatically filters out rows where the `total_batches_created` column has a missing (`NaN`/`None`) value before display and SQL generation.
* **Interactive Data Table:** Displays parsed data using the DataTables jQuery plugin, providing:
    * Pagination
    * Filtering/Searching
    * Option to show 10, 25, 50, 100, or "All" entries per page.
* **Column Visibility Control:** Integrates DataTables Buttons extension to allow users to dynamically show/hide columns via a "Column visibility" button.
* **Default Columns:** Configured to show a specific set of columns by default upon loading the results page.
* **External Sorting:** Provides a dropdown menu to sort the table data by any column (ascending).
* **SQL Generation:** Generates PostgreSQL `INSERT` statements for the `public.process_queue_wait_times` table based on the parsed (and filtered) data.
    * Includes a `TRUNCATE TABLE` statement.
    * Filters columns based on a predefined `VALID_COLUMNS` set.
    * **Note:** The SQL value formatting deliberately mimics older script logic, which may not produce standard SQL `NULL` for missing values and does not escape single quotes within string data.

## Requirements

* Python 3.x
* Flask
* pandas
* cachelib

## Setup & Installation

1.  **Clone or Download:** Get the project files (`app.py`, `templates/index.html`, `templates/results.html`).
2.  **Navigate:** Open a terminal or command prompt and navigate to the project directory.
3.  **Virtual Environment (Recommended):**
    * Create a virtual environment: `python -m venv venv`
    * Activate it:
        * Windows: `.\venv\Scripts\activate`
        * macOS/Linux: `source venv/bin/activate`
4.  **Install Dependencies:**
    ```bash
    pip install Flask pandas cachelib
    ```

## Usage

1.  **Run the Application:**
    ```bash
    python app.py
    ```
2.  **Access:** Open your web browser and go to `http://127.0.0.1:5000` (or the address shown in the terminal, possibly `http://0.0.0.0:5000`).
3.  **Input Data:**
    * Paste the log content directly into the text area. **Note:** Very large pastes might be rejected due to server request size limits (see Configuration).
    * Alternatively, click "Choose File" / "Browse" to select and upload a log file. This is recommended for large logs.
4.  **Parse:** Click the "Parse Logs" button.
5.  **View Results:** If parsing is successful, you will be redirected to the results page.
    * The data is displayed in an interactive table.
    * Use the "Show X entries" dropdown to control pagination (includes "All").
    * Use the "Search" box to filter data across all columns.
    * Use the "Sort by" dropdown to sort the table by a specific column.
    * Use the "Column visibility" button to select which columns are displayed.
6.  **Generate SQL:** Click the "Generate PostgreSQL INSERTs" button. The generated SQL statements (including the initial `TRUNCATE`) will appear in the text area below the button.

## Configuration (in `app.py`)

Several aspects can be configured by editing `app.py`:

* **`app.config['MAX_CONTENT_LENGTH']`**: Sets the maximum request size (e.g., for pasted text). Defaults to 50MB. Note that production web servers (Nginx, Apache) might have their own lower limits.
* **`CACHE_TIMEOUT`**: Duration (in seconds) for which parsed data is stored in the server's memory cache. Defaults to 3600 (1 hour).
* **`VALID_COLUMNS`**: A Python `set` containing the names of columns that are considered valid for inclusion in the generated SQL `INSERT` statements. Ensure this matches your target table schema.
* **`parse_log_stream()`**: Contains the core parsing logic. The `original_header_start` and `original_kv_start` variables should exactly match the format of your log files.
* **`/results` route:**
    * `filter_column`: The column used to filter out rows with missing data (`'total_batches_created'`).
    * `default_visible_columns`: A Python `list` of column names that should be visible by default on the results page. Verify these names exist in your parsed data.
* **`generate_sql_inserts()`**:
    * `nan_check_column`: The column used to skip rows during SQL generation if the value is missing (`'total_batches_created'`).
    * The target table name (`public.process_queue_wait_times`) is hardcoded in the `TRUNCATE` and `INSERT` statements.

## Notes & Potential Issues

* **SQL Formatting:** The `generate_sql_inserts` function intentionally uses simple value formatting to match legacy behavior. This means `None`/`NaN` values become string literals like `'None'` or `'nan'` instead of SQL `NULL`, and single quotes within string data are *not* escaped, which can cause SQL syntax errors.
* **Log Format Specificity:** The parser (`parse_log_stream`) is tightly coupled to the specific format of the input logs (lines starting with `│...│`, specific key-value structures). Changes in the log format will likely break the parser.
* **Request Size Limit:** While the app handles large *file uploads* well, pasting extremely large amounts of text can still hit the `MAX_CONTENT_LENGTH` limit or underlying web server limits, resulting in a "Request Entity Too Large" error. Use file uploads for large inputs.
* **Default Columns:** Ensure the column names listed in `default_visible_columns` within the `/results` route in `app.py` exactly match the column names produced by the parser after any prefixing (e.g., `source_table_name`, not `sourceName`).