# OracleToMySQLConverter

This tool helps you to convert Oracle table schema into MySQL schema and import into MYSQL database server.

Configration Details:

All the configurations are maintained in app.config file

Add Oracle connection string
<add name="OracleConnection" connectionString="Server=orcl;User ID=system;Password=password;Min Pool Size=0;Max Pool Size=300;Connection Lifetime=15" providerName="System.Data.OracleClient"/>

Add Oracle schema name
<add key="OracleSchemaName" value="SYSTEM"/>

To import schema into MySQL server enable this key value to TRUE.
<add key="ImportSchemaToMySQLServer" value="FALSE"/>

Add MySQL connection for db import.
<add name="MySQLConnection" connectionString="Datasource=localhost;Database=test;uid=test;pwd=password;Allow User Variables=True" providerName="MySql.Data.MySqlClient"/>
