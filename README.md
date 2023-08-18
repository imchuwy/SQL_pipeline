# SQL_pipeline

SQL files for a scheduled pipeline. This automaticically pipeline refreshed tables on a specified increment.
This is done by calling a stored procedure which triggers the running of a SQL view to update the core table.
Archetecture AWS glue > SNS > Stored procedure > Call view
