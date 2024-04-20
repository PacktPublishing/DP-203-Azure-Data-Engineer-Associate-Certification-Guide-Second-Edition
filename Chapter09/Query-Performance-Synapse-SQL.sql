-- Querying the system tables
-- Synapse SQL Pool provides the following system tables that can be used to monitor the query performance:

-- sys.dm_pdw_exec_requests – contains all the current and recently active requests in Azure Synapse Analytics. It contains details like total_elapsed_time, submit_time, start_time, end_time, command, result_cache_hit and so on.
SELECT * FROM sys.dm_pdw_exec_requests;

-- sys.dm_pdw_waits – contains details of the wait states in a query, including locks and waits on transmission queues. 
SELECT * FROM sys.dm_pdw_waits;


-- Using Query Store

-- sys.query_store_query - contains information about queries captured by Query Store
SELECT * FROM sys.query_store_query;

-- sys.query_store_query_text - contains the text of queries captured by Query Store
SELECT * FROM sys.query_store_query_text;

-- Query Plan

-- sys.query_store_plan - contains information about query plans captured by Query Store
SELECT * FROM sys.query_store_plan;

-- Runtime Statistics

-- sys.query_store_runtime_stats - contains runtime statistics for queries captured by Query Store
SELECT * FROM sys.query_store_runtime_stats;

-- sys.query_store_runtime_stats_interval - contains intervals of runtime statistics for queries captured by Query Store
SELECT * FROM sys.query_store_runtime_stats_interval;


