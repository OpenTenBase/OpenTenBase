## Project Name

## Project Introduction

The `slowquery` plugin is a performance monitoring tool efficiently integrated with PostgreSQL, focusing on automatically identifying and recording SQL queries that exceed the preset threshold of execution time. With flexible configuration through GUC parameters, it adapts to various performance monitoring needs. The plugin utilizes PostgreSQL's hook mechanism and memory management to accurately track query execution times, outputting detailed log information when thresholds are exceeded, assisting database administrators in performance analysis and optimization.

## Running Instructions

### Environment Preparation

- Ensure that you have PostgreSQL database installed on your system and that you have access to the database.
- Confirm that your PostgreSQL version is compatible with your plugin.
- Make sure your user account has sufficient permissions to install new extensions.

### Installing slowquery

1. Place your plugin source code in the `${SOURCECODE_PATH}/contrib` directory.

2. Ensure that the source code includes a `Makefile` and the necessary C/C++ source files.

3. Open a terminal and switch to the plugin source code directory.

4. Run the `make` command to compile the plugin:

   ```
   make
   ```

### Configuration

1. Set the threshold for slow queries, for example, to find historical SQL that takes longer than 10 seconds, add to `postgresql.conf`:

   ```
   slowquery_min_time = '10000'
   ```

2. Reload the configuration or restart the database to apply changes, with the command:

   ```
   sudo systemctl start postgresql
   ```

3. Create the database extension: connect to the database using `psql` or another PostgreSQL client tool and run the following command:

   ```
   CREATE EXTENSION slowquery;
   ```

### Usage

The plugin will automatically record queries that exceed the set threshold and log detailed information in the PostgreSQL log.

### Viewing Logs

Check the PostgreSQL server's log files for slow query records.

1. **Determine Log Location**: Check the `log_directory` parameter in the `postgresql.conf` configuration file to determine the directory where log files are stored.

2. **View Log Files**: Use a text editor or command-line tool (such as `tail`, `less`, or `grep`) to view the current log file or historical log files. For example, use the following command to monitor new log entries in real-time:

   ```
   tail -f /path/to/pg_log/postgresql-*.log
   ```

3. **Search Slow Query Logs**: If you need to find specific slow query records, use the `grep` command to filter the content in the log files. For example:

   ```
   grep 'slow query' /path/to/pg_log/postgresql-*.log
   ```

4. **Analyze Log Content**: Analyze the slow query logs to identify slow-performing SQL statements and optimize as needed.

### Uninstalling

- If you need to uninstall the plugin, you can do so by running the following SQL command:

  ```
  DROP EXTENSION slowquery;
  ```

## Testing Instructions

### Testing Steps

1. Connect to the PostgreSQL database.

   Use `psql` or another database client tool to connect to the PostgreSQL database.

2. Execute a query that is expected to have a short execution time, such as:

   ```
   SELECT COUNT(*) FROM users;
   ```

   This query should be executed quickly and not trigger slow query log recording.

3. Execute a query that is expected to have a long execution time, such as a full table scan or a complex JOIN operation:

   ```
   SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table);
   ```

   If the query execution time exceeds 10 seconds, it should be captured and recorded by the `slowquery` plugin.

4. To ensure that the query in step 3 indeed exceeds the threshold, use the `pg_sleep` function to simulate a long execution time, such as:

   ```
   SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table ORDER BY (SELECT pg_sleep(5)));
   ```

   This query simulates a 5-second delay with `pg_sleep(5)` to ensure that the query execution time exceeds 10 seconds.

5. After completing all queries, check the PostgreSQL log files for slow query records.

6. Use the `grep` command to filter slow query log entries, for example:

   ```
   grep 'slow query' /path/to/pg_log/postgresql-*.log
   ```

Note: If the data volume in `large_table` in the test environment is not sufficient to produce slow queries, you can increase the data volume or adjust the delay time of `pg_sleep` to ensure the effectiveness of the test.

### Expected Results

- Queries with short execution times (step 2) will not be recorded in the slow query log.
- Queries with long execution times (steps 3 and 4) will be recorded in the slow query log, and the records should include the text of the query and the execution time.

### Verification Method

Search for slow query records in the PostgreSQL log files, for example:

Use command-line tools (such as `grep`) to search for log entries containing slow query information. For example, you can use the following command:

```
grep 'slow query' /path/to/pg_log/postgresql-*.log
```

```
LOG: 2024-04-06 10:00:00.000 UTC [slow query]  SELECT * FROM large_table WHERE some_column IN (SELECT some_id FROM another_table ORDER BY (SELECT pg_sleep(5)));  10s
```

## Technical Architecture

The `slowquery` plugin monitors and records slow queries exceeding a specified threshold through GUC configuration, execution-time hooks, memory contexts, and log recording.