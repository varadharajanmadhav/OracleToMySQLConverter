/**
* @author Varadharajan.N.M
* Software Architect
* Aureole Technologies Pvt Ltd
* 
*/

using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;

namespace OracleToMySQLConvertor
{
    class Program
    {
        static string FolderPath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\Scripts";
        static string LogFilePath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\log_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
        static string OracleSchemaName = ConfigurationManager.AppSettings["OracleSchemaName"].ToString().Trim().ToUpper();

        static int TableIndex = 0;
        static int TableCount = 0;
        static List<string> CompletedTables = new List<string>();

        static void Main(string[] args)
        {
            try
            {
                //Creating scripts folder for saving multiple database script files
                if (!Directory.Exists(FolderPath))
                    Directory.CreateDirectory(FolderPath);

                while (true)
                {
                    Console.WriteLine("1. Generate & Import MySQL Script");
                    Console.WriteLine("2. Generate MySQL Script");
                    Console.WriteLine("3. Generate Oracle Script");
                    Console.WriteLine("4. Execute Oracle Script from file");
                    Console.WriteLine("5. Exit");
                    Console.WriteLine("Enter your choice:");

                    TableIndex = 0;
                    TableCount = 0;
                    CompletedTables = new List<string>();

                    string input = Console.ReadLine();
                    if (input == "1")
                    {
                        GenerateAndImportMySQLScript(true, string.Empty);
                    }
                    else if (input == "2")
                    {
                        Console.WriteLine("Do you want to generate script in single file? Y-yes, N-No");
                        string ans = Console.ReadLine();
                        string fileName = string.Empty;
                        if (ans.Trim().ToUpper() == "Y")
                            fileName = FolderPath + "\\MySQLDBScript_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
                        GenerateAndImportMySQLScript(false, fileName);
                    }
                    else if (input == "3")
                    {
                        Console.WriteLine("Do you want to generate script in single file? Y-yes, N-No");
                        string ans = Console.ReadLine();
                        string fileName = string.Empty;
                        if (ans.Trim().ToUpper() == "Y")
                            fileName = FolderPath + "\\OracleDBScript_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
                        GenerateOracleScript(fileName);
                    }
                    else if (input == "4")
                    {
                        ExecuteOracleScript();
                    }
                    else if (input == "5")
                    {
                        break;
                    }
                    else
                    {
                        Console.WriteLine("Invalid Choice");
                    }
                }                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.ReadLine();
        }

        static void WriteToFile(string tableName, string content)
        {
            string fileName = FolderPath + "\\" + tableName + ".txt";
            System.IO.File.WriteAllText(fileName, content);
        }

        static void AppendToFile(string fileName, string content)
        {
            System.IO.File.AppendAllText(fileName, content + "\n\n");
        }

        static void WriteLog(string content)
        {
            Console.WriteLine(content);
            System.IO.File.AppendAllText(LogFilePath, content + "\n");
        }

        #region Generate MySQL Script

        static void GenerateAndImportMySQLScript(bool importMySQLScript, string singleFileName)
        {
            try
            {
                Database db = DatabaseFactory.CreateDatabase("OracleConnection");
                Database mySQLDB = DatabaseFactory.CreateDatabase("MySQLConnection");
                DataTable tableList = DAL.GetTableList(db);
                TableCount = tableList.Rows.Count;

                //Populating auto increment column list from csv file
                Dictionary<string, string> autoIncrementColList = PopulateAutoIncrementColumnList(mySQLDB);

                foreach (DataRow tableRow in tableList.Rows)
                {
                    string tableName = tableRow["TABLE_NAME"].ToString().ToUpper();
                    if (!CompletedTables.Contains(tableName))
                        GenerateMySQLScriptForTable(db, mySQLDB, tableName, importMySQLScript, singleFileName, autoIncrementColList);
                }

                //DataTable viewList = DAL.GetViewDetails(db, OracleSchemaName);
                //foreach (DataRow row in viewList.Rows)
                //{
                //    string viewName = row["VIEW_NAME"].ToString().Trim();
                //    string viewText = row["TEXT"].ToString();

                //    WriteLog("Creating View " + viewName + "...");
                //    StringBuilder schemaBuilder = new StringBuilder();
                //    schemaBuilder.AppendFormat("CREATE OR REPLACE VIEW {0} AS {1}", viewName, DAL.ParseQueryToMySQLDB(viewText.Replace("--","-- ").Replace(", ", ",")));

                //    try
                //    {
                //        DAL.ExecuteMySQLScript(mySQLDB, schemaBuilder.ToString());
                //    }
                //    catch(Exception ex)
                //    {
                //        schemaBuilder.Append("\n-- Exception: " + ex.Message);
                //        WriteLog(ex.Message);
                //    }

                //    WriteToFile(viewName, schemaBuilder.ToString());
                //}

                WriteLog("Completed.");
            }
            catch
            {
                throw;
            }
        }

        static void GenerateMySQLScriptForTable(Database db, Database mySQLDB, string tableName, bool importToMySQL, string singleFileName, Dictionary<string,string> autoIncrementColList)
        {
            string logDesc = string.Empty;
            if (importToMySQL)
                logDesc = string.Format("Importing Table {0} ({1}/{2})...", tableName, ++TableIndex, TableCount);
            else
                logDesc = string.Format("Generating database script for {0} ({1}/{2})...", tableName, ++TableIndex, TableCount);
            WriteLog(logDesc);

            DataTable columnTable = DAL.GetTableColumns(db, OracleSchemaName, tableName);
            DataTable primaryKeyTable = DAL.GetPrimaryKeyDetails(db, OracleSchemaName, tableName, string.Empty);
            DataTable uniqueKeyTable = DAL.GetUniqueKeyDetails(db, OracleSchemaName, tableName);
            DataTable foreignKeyTable = DAL.GetForeignKeyDetails(db, OracleSchemaName, tableName);
            DataTable indexTable = DAL.GetIndexDetails(db, OracleSchemaName, tableName);

            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.AppendFormat("CREATE TABLE {0} \n(", tableName);
            int colIndex = 0;
            foreach (DataRow colRow in columnTable.Rows)
            {
                //add new line for each column
                schemaBuilder.Append("\n");

                string columnName = colRow["COLUMN_NAME"].ToString().ToUpper();
                string dataType = colRow["DATA_TYPE"].ToString().ToUpper();
                int dataLength = 0;
                int.TryParse(colRow["DATA_LENGTH"].ToString(), out dataLength);
                int dataPrecision = 0;
                int.TryParse(colRow["DATA_PRECISION"].ToString(), out dataPrecision);
                int dataScale = 0;
                int.TryParse(colRow["DATA_SCALE"].ToString(), out dataScale);
                string nullable = colRow["NULLABLE"].ToString().ToUpper();
                string dataDefault = colRow["DATA_DEFAULT"].ToString().Length > 0 ? colRow["DATA_DEFAULT"].ToString() : "NULL";

                if (colIndex > 0)
                    schemaBuilder.Append(",");

                if (dataType.Contains("TIMESTAMP") || columnName.Contains("FTIMESTAMP"))
                    schemaBuilder.AppendFormat("{0}\t TIMESTAMP", columnName);
                else if (dataType == "VARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tVARCHAR({1})", columnName, dataLength);
                else if (dataType == "NVARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tNVARCHAR({1})", columnName, dataLength / 2);//in oracle it doubles the datalength of NVARCHAR2
                else if (dataType == "CHAR")
                    schemaBuilder.AppendFormat("{0}\tCHAR({1})", columnName, dataLength);
                else if (dataType == "NUMBER" && dataScale == 0 & dataPrecision > 13)
                    schemaBuilder.AppendFormat("{0}\tBIGINT", columnName);
                else if (dataType == "NUMBER" && dataScale == 0)
                    schemaBuilder.AppendFormat("{0}\tINT", columnName);
                else if (dataType == "NUMBER" && dataScale > 0)
                    schemaBuilder.AppendFormat("{0}\tDECIMAL({1},{2})", columnName, dataPrecision, dataScale);
                else if (dataType == "NCLOB" || dataType == "CLOB")
                    schemaBuilder.AppendFormat("{0}\tMEDIUMTEXT", columnName);
                else
                    schemaBuilder.AppendFormat("{0}\t{1}", columnName, dataType);

                if (nullable == "N")
                    schemaBuilder.Append("\tNOT NULL");
                else
                {
                    if (dataType.Contains("TIMESTAMP") || columnName.Contains("FTIMESTAMP") || dataDefault.Contains("TIMESTAMP"))
                        schemaBuilder.AppendFormat("\tDEFAULT CURRENT_TIMESTAMP");
                    else if (dataDefault.Contains("TO_CHAR") || dataDefault.Contains("SYSDATE"))
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", "(" + DAL.ParseQueryToMySQLDB(dataDefault) + ")");
                    else if(dataDefault.Contains("--"))
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault.Substring(0, dataDefault.IndexOf("--")).Trim());
                    else
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault.Trim());
                }

                if (autoIncrementColList.ContainsKey(tableName))
                {
                    string autoIncrementCol = autoIncrementColList[tableName];
                    if (autoIncrementCol.ToUpper() == columnName)
                        schemaBuilder.Append("\tAUTO_INCREMENT");
                }

                colIndex++;
            }

            List<string> constraintList = new List<string>();

            if (primaryKeyTable.Rows.Count > 0)
            {
                string constraintName = primaryKeyTable.Rows[0]["CONSTRAINT_NAME"].ToString().ToUpper();
                List<string> columnList = new List<string>();
                foreach (DataRow primaryKeyRow in primaryKeyTable.Rows)
                {
                    string columnName = primaryKeyRow["COLUMN_NAME"].ToString().ToUpper();
                    columnList.Add(columnName);
                }
                schemaBuilder.AppendFormat("\n,CONSTRAINT {0} PRIMARY KEY ({1})", constraintName, string.Join(",", columnList));
                constraintList.Add(constraintName);
            }

            if (uniqueKeyTable.Rows.Count > 0)
            {
                DataTable uniqueKeys = uniqueKeyTable.DefaultView.ToTable(true, "CONSTRAINT_NAME");
                foreach (DataRow uniqueKeyRow in uniqueKeys.Rows)
                {
                    string constraintName = uniqueKeyRow["CONSTRAINT_NAME"].ToString();
                    DataRow[] uniqueKeyColumns = uniqueKeyTable.Select("CONSTRAINT_NAME='" + constraintName + "'", "POSITION ASC");
                    List<string> columnList = new List<string>();
                    foreach (DataRow row in uniqueKeyColumns)
                    {
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        columnList.Add(columnName);
                    }
                    schemaBuilder.AppendFormat("\n,CONSTRAINT {0} UNIQUE KEY ({1})", constraintName, string.Join(",", columnList));
                    constraintList.Add(constraintName);
                }
            }

            List<string> foreignKeyTables = new List<string>();
            if (foreignKeyTable.Rows.Count > 0)
            {
                DataTable uniqueForeignKeys = foreignKeyTable.DefaultView.ToTable(true, "CONSTRAINT_NAME");
                foreach (DataRow uniqueKeyRow in uniqueForeignKeys.Rows)
                {
                    string constraintName = uniqueKeyRow["CONSTRAINT_NAME"].ToString();
                    DataRow[] uniqueKeyColumns = foreignKeyTable.Select("CONSTRAINT_NAME='" + constraintName + "'", "POSITION ASC");

                    string rContraintName = string.Empty;
                    List<string> columnList = new List<string>();
                    foreach (DataRow row in uniqueKeyColumns)
                    {
                        rContraintName = row["R_CONSTRAINT_NAME"].ToString().ToUpper();
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        columnList.Add(columnName);
                    }

                    string rTableName = string.Empty;
                    List<string> rColumnList = new List<string>();
                    DataTable primaryKeyInfoTable = DAL.GetPrimaryKeyDetails(db, OracleSchemaName, string.Empty, rContraintName);
                    foreach(DataRow row in primaryKeyInfoTable.Rows)
                    {
                        rTableName = row["TABLE_NAME"].ToString().Trim();
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        rColumnList.Add(columnName);
                    }

                    schemaBuilder.AppendFormat("\n,CONSTRAINT {0} FOREIGN KEY ({1}) REFERENCES {2}({3})", constraintName, string.Join(",", columnList), rTableName, string.Join(",", rColumnList));
                    foreignKeyTables.Add(rTableName);
                }
            }

            //create foreign key tables first
            foreach(string forignkeyTable in foreignKeyTables)
            {
                if (!CompletedTables.Contains(forignkeyTable))
                {
                    WriteLog("Executing Foreign key table " + forignkeyTable);
                    GenerateMySQLScriptForTable(db, mySQLDB, forignkeyTable, importToMySQL, singleFileName, autoIncrementColList);
                }
            }

            schemaBuilder.Append("\n);");

            if (indexTable.Rows.Count > 0)
            {
                DataTable uniqueIndexs = indexTable.DefaultView.ToTable(true, "INDEX_NAME");
                foreach (DataRow uniqueIndexRow in uniqueIndexs.Rows)
                {
                    string indexName = uniqueIndexRow["INDEX_NAME"].ToString().ToUpper();
                    if (!constraintList.Contains(indexName))
                    {
                        schemaBuilder.Append("\n");
                        DataRow[] indexColumns = indexTable.Select("INDEX_NAME='" + indexName + "'", "COLUMN_POSITION ASC");
                        string indexType = string.Empty;
                        List<string> columnExpressionList = new List<string>();
                        List<string> columnList = new List<string>();
                        foreach (DataRow row in indexColumns)
                        {
                            indexType = row["INDEX_TYPE"].ToString().ToUpper();
                            string columnExpression = row["COLUMN_EXPRESSION"].ToString().ToUpper().Replace("\"", "");
                            string columnName = row["COLUMN_NAME"].ToString().ToUpper();

                            if (columnExpression.Trim().Length > 0)
                                columnList.Add(columnExpression);
                            else
                                columnList.Add(columnName);
                        }

                        if (indexType != "FUNCTION-BASED NORMAL")
                            schemaBuilder.AppendFormat("\nCREATE INDEX {0} ON {1}({2});", indexName, tableName, string.Join(",", columnList));
                        else
                        {
                            List<string> modifiedExpList = new List<string>();
                            foreach (string colExp in columnList)
                            {
                                if (colExp.Contains("NVL") || colExp.Contains("DECODE") || colExp.Contains("UPPER") || colExp.Contains("TO_CHAR") || colExp.Contains("TO_DATE") || colExp.Contains("-") || colExp.Contains("+"))
                                    modifiedExpList.Add("(" + DAL.ParseQueryToMySQLDB(colExp) + ")");//Outer bracket required for functional expression
                                else
                                    modifiedExpList.Add(colExp);
                            }
                            schemaBuilder.AppendFormat("\nCREATE INDEX {0} ON {1}({2});", indexName, tableName, string.Join(",", modifiedExpList));
                        }
                    }
                }
            }

            StringBuilder schemaLogBuilder = new StringBuilder();

            //Import data to MySQL Server
            if (importToMySQL)
            {
                var queries = schemaBuilder.ToString().Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                foreach (string query in queries)
                {
                    try
                    {
                        DAL.ExecuteDatabaseScript(mySQLDB, query);
                        schemaLogBuilder.Append(query + ";");
                    }
                    catch(Exception ex)
                    {
                        schemaLogBuilder.Append(query + ";");
                        schemaLogBuilder.Append("\n-- Exception: " + ex.Message);
                        WriteLog(tableName + ": " + ex.Message);
                    }
                }
            }
            else
            {
                schemaLogBuilder.Append(schemaBuilder.ToString());
            }

            //Write table schema into file.
            if (singleFileName.Length == 0)
                WriteToFile(tableName, schemaLogBuilder.ToString());
            else
                AppendToFile(singleFileName, schemaLogBuilder.ToString());

            CompletedTables.Add(tableName);
        }

        static Dictionary<string, string> PopulateAutoIncrementColumnList(Database mySQLDB)
        {
            try
            {
                Dictionary<string, string> autoIncrementColList = new Dictionary<string, string>();
                string autoIncrementListFile = ConfigurationManager.AppSettings["AutoIncrementListCsvFile"].ToString();
                if (File.Exists(autoIncrementListFile))
                {
                    using (StreamReader sr = File.OpenText(autoIncrementListFile))
                    {
                        string s = String.Empty;
                        while ((s = sr.ReadLine()) != null)
                        {
                            if (s.Trim().Length > 0)
                            {
                                string[] rowValues = s.Trim().Split(",".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                                string tableName = rowValues[0].Trim();
                                string columnName = rowValues[1].Trim();
                                autoIncrementColList.Add(tableName, columnName);
                            }
                        }
                    }
                }

                return autoIncrementColList;
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region Generate Oracle Script

        static void GenerateOracleScript(string singleFileName)
        {
            try
            {
                Database db = DatabaseFactory.CreateDatabase("OracleConnection");
                DataTable tableList = DAL.GetTableList(db);
                TableCount = tableList.Rows.Count;

                //Generate table script
                foreach (DataRow tableRow in tableList.Rows)
                {
                    string tableName = tableRow["TABLE_NAME"].ToString().ToUpper();
                    if (!CompletedTables.Contains(tableName))
                        GenerateOracleScriptForTable(db, tableName, singleFileName);
                }

                //Generate view script
                DataTable viewList = DAL.GetViewDetails(db, OracleSchemaName);
                foreach (DataRow row in viewList.Rows)
                {
                    string viewName = row["VIEW_NAME"].ToString().Trim();
                    string viewText = row["TEXT"].ToString();

                    WriteLog("Generating database script for " + viewName + "...");
                    StringBuilder schemaBuilder = new StringBuilder();
                    schemaBuilder.AppendFormat("CREATE OR REPLACE VIEW {0} AS {1};", viewName, viewText.Replace("--", "-- "));

                    if (singleFileName.Length == 0)
                        WriteToFile(viewName, schemaBuilder.ToString());
                    else
                        AppendToFile(singleFileName, schemaBuilder.ToString());
                }

                //Generate sequence script
                DataTable sequenceTable = DAL.GetSequenceDetails(db);
                foreach(DataRow row in sequenceTable.Rows)
                {
                    string sequenceName = row["SEQUENCE_NAME"].ToString();

                    WriteLog("Generating database script for " + sequenceName + "...");
                    StringBuilder schemaBuilder = new StringBuilder();
                    schemaBuilder.AppendFormat("CREATE SEQUENCE {0} START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;", sequenceName);

                    if (singleFileName.Length == 0)
                        WriteToFile(sequenceName, schemaBuilder.ToString());
                    else
                        AppendToFile(singleFileName, schemaBuilder.ToString());
                }

                WriteLog("Completed.");
            }
            catch
            {
                throw;
            }
        }

        static void GenerateOracleScriptForTable(Database db, string tableName, string singleFileName)
        {
            string logDesc = string.Format("Generating database script for {0} ({1}/{2})...", tableName, ++TableIndex, TableCount);
            WriteLog(logDesc);

            DataTable columnTable = DAL.GetTableColumns(db, OracleSchemaName, tableName);
            DataTable primaryKeyTable = DAL.GetPrimaryKeyDetails(db, OracleSchemaName, tableName, string.Empty);
            DataTable uniqueKeyTable = DAL.GetUniqueKeyDetails(db, OracleSchemaName, tableName);
            DataTable foreignKeyTable = DAL.GetForeignKeyDetails(db, OracleSchemaName, tableName);
            DataTable indexTable = DAL.GetIndexDetails(db, OracleSchemaName, tableName);

            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.AppendFormat("CREATE TABLE {0} \n(", tableName);
            int colIndex = 0;
            foreach (DataRow colRow in columnTable.Rows)
            {
                //add new line for each column
                schemaBuilder.Append("\n");

                string columnName = colRow["COLUMN_NAME"].ToString().ToUpper();
                string dataType = colRow["DATA_TYPE"].ToString().ToUpper();
                int dataLength = 0;
                int.TryParse(colRow["DATA_LENGTH"].ToString(), out dataLength);
                int dataPrecision = 0;
                int.TryParse(colRow["DATA_PRECISION"].ToString(), out dataPrecision);
                int dataScale = 0;
                int.TryParse(colRow["DATA_SCALE"].ToString(), out dataScale);
                string nullable = colRow["NULLABLE"].ToString().ToUpper();
                string dataDefault = colRow["DATA_DEFAULT"].ToString().Length > 0 ? colRow["DATA_DEFAULT"].ToString() : "NULL";

                if (colIndex > 0)
                    schemaBuilder.Append(",");

                if (dataType.Contains("TIMESTAMP") || columnName.Contains("FTIMESTAMP"))
                    schemaBuilder.AppendFormat("{0}\t TIMESTAMP", columnName);
                else if (dataType == "VARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tVARCHAR2({1})", columnName, dataLength);
                else if (dataType == "NVARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tNVARCHAR2({1})", columnName, dataLength / 2);//in oracle it doubles the datalength of NVARCHAR2
                else if (dataType == "CHAR")
                    schemaBuilder.AppendFormat("{0}\tCHAR({1})", columnName, dataLength);
                else if (dataType == "NUMBER")
                    schemaBuilder.AppendFormat("{0}\tNUMBER({1},{2})", columnName, dataPrecision, dataScale);
                else
                    schemaBuilder.AppendFormat("{0}\t{1}", columnName, dataType);

                if (nullable == "N")
                    schemaBuilder.Append("\tNOT NULL");
                else
                {
                    if (dataType.Contains("TIMESTAMP") || columnName.Contains("FTIMESTAMP") || dataDefault.Contains("TIMESTAMP"))
                        schemaBuilder.AppendFormat("\tDEFAULT CURRENT_TIMESTAMP");
                    else if (dataDefault.Contains("--") && dataDefault.Contains(";"))
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault.Trim().Replace(";", " "));
                    else
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault.Trim());
                }

                colIndex++;
            }

            List<string> constraintList = new List<string>();

            if (primaryKeyTable.Rows.Count > 0)
            {
                string constraintName = primaryKeyTable.Rows[0]["CONSTRAINT_NAME"].ToString().ToUpper();
                List<string> columnList = new List<string>();
                foreach (DataRow primaryKeyRow in primaryKeyTable.Rows)
                {
                    string columnName = primaryKeyRow["COLUMN_NAME"].ToString().ToUpper();
                    columnList.Add(columnName);
                }
                schemaBuilder.AppendFormat("\n,CONSTRAINT {0} PRIMARY KEY ({1})", constraintName, string.Join(",", columnList));
                constraintList.Add(constraintName);
            }

            if (uniqueKeyTable.Rows.Count > 0)
            {
                DataTable uniqueKeys = uniqueKeyTable.DefaultView.ToTable(true, "CONSTRAINT_NAME");
                foreach (DataRow uniqueKeyRow in uniqueKeys.Rows)
                {
                    string constraintName = uniqueKeyRow["CONSTRAINT_NAME"].ToString();
                    DataRow[] uniqueKeyColumns = uniqueKeyTable.Select("CONSTRAINT_NAME='" + constraintName + "'", "POSITION ASC");
                    List<string> columnList = new List<string>();
                    foreach (DataRow row in uniqueKeyColumns)
                    {
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        columnList.Add(columnName);
                    }
                    schemaBuilder.AppendFormat("\n,CONSTRAINT {0} UNIQUE KEY ({1})", constraintName, string.Join(",", columnList));
                    constraintList.Add(constraintName);
                }
            }

            List<string> foreignKeyTables = new List<string>();
            if (foreignKeyTable.Rows.Count > 0)
            {
                DataTable uniqueForeignKeys = foreignKeyTable.DefaultView.ToTable(true, "CONSTRAINT_NAME");
                foreach (DataRow uniqueKeyRow in uniqueForeignKeys.Rows)
                {
                    string constraintName = uniqueKeyRow["CONSTRAINT_NAME"].ToString();
                    DataRow[] uniqueKeyColumns = foreignKeyTable.Select("CONSTRAINT_NAME='" + constraintName + "'", "POSITION ASC");

                    string rContraintName = string.Empty;
                    List<string> columnList = new List<string>();
                    foreach (DataRow row in uniqueKeyColumns)
                    {
                        rContraintName = row["R_CONSTRAINT_NAME"].ToString().ToUpper();
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        columnList.Add(columnName);
                    }

                    string rTableName = string.Empty;
                    List<string> rColumnList = new List<string>();
                    DataTable primaryKeyInfoTable = DAL.GetPrimaryKeyDetails(db, OracleSchemaName, string.Empty, rContraintName);
                    foreach (DataRow row in primaryKeyInfoTable.Rows)
                    {
                        rTableName = row["TABLE_NAME"].ToString().Trim();
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        rColumnList.Add(columnName);
                    }

                    schemaBuilder.AppendFormat("\n,CONSTRAINT {0} FOREIGN KEY ({1}) REFERENCES {2}({3})", constraintName, string.Join(",", columnList), rTableName, string.Join(",", rColumnList));
                    foreignKeyTables.Add(rTableName);
                }
            }

            //create foreign key tables first
            foreach (string forignkeyTable in foreignKeyTables)
            {
                if (!CompletedTables.Contains(forignkeyTable))
                {
                    WriteLog("Executing Foreign key table " + forignkeyTable);
                    GenerateOracleScriptForTable(db, forignkeyTable, singleFileName);
                }
            }

            schemaBuilder.Append("\n);");

            if (indexTable.Rows.Count > 0)
            {
                DataTable uniqueIndexs = indexTable.DefaultView.ToTable(true, "INDEX_NAME");
                foreach (DataRow uniqueIndexRow in uniqueIndexs.Rows)
                {
                    string indexName = uniqueIndexRow["INDEX_NAME"].ToString().ToUpper();
                    if (!constraintList.Contains(indexName))
                    {
                        schemaBuilder.Append("\n");
                        DataRow[] indexColumns = indexTable.Select("INDEX_NAME='" + indexName + "'", "COLUMN_POSITION ASC");
                        string indexType = string.Empty;
                        List<string> columnExpressionList = new List<string>();
                        List<string> columnList = new List<string>();
                        foreach (DataRow row in indexColumns)
                        {
                            indexType = row["INDEX_TYPE"].ToString().ToUpper();
                            string columnExpression = row["COLUMN_EXPRESSION"].ToString().ToUpper().Replace("\"", "");
                            string columnName = row["COLUMN_NAME"].ToString().ToUpper();

                            if (columnExpression.Trim().Length > 0)
                                columnList.Add(columnExpression);
                            else
                                columnList.Add(columnName);
                        }

                        schemaBuilder.AppendFormat("\nCREATE INDEX {0} ON {1}({2});", indexName, tableName, string.Join(",", columnList));
                    }
                }
            }

            //Write table schema into file.
            if (singleFileName.Length == 0)
                WriteToFile(tableName, schemaBuilder.ToString());
            else
                AppendToFile(singleFileName, schemaBuilder.ToString());

            CompletedTables.Add(tableName);
        }

        #endregion

        static void ExecuteOracleScript()
        {
            try
            {
                Database db = DatabaseFactory.CreateDatabase("OracleConnection");
                Console.WriteLine("Enter sql file path:");
                string filePath = Console.ReadLine();

                if (File.Exists(filePath))
                {
                    using (StreamReader sr = File.OpenText(filePath))
                    {
                        StringBuilder query = new StringBuilder();
                        string s = String.Empty;
                        while ((s = sr.ReadLine()) != null)
                        {
                            if (s.Trim().Length > 0)
                            {
                                query.Append(s);

                                if (s.Trim().Substring(s.Trim().Length - 1) == ";")
                                {
                                    try
                                    {
                                        DAL.ExecuteDatabaseScript(db, query.ToString());
                                    }
                                    catch (Exception ex)
                                    {
                                        StringBuilder schemaLogBuilder = new StringBuilder();
                                        schemaLogBuilder.Append(query);
                                        schemaLogBuilder.Append("\n-- Exception: " + ex.Message);
                                        WriteLog(ex.Message + "\n");
                                    }
                                    query.Clear();
                                }
                            }
                        }
                    }
                    WriteLog("Completed.");
                }
                else
                {
                    Console.WriteLine("File doesn't exists");
                }
            }
            catch
            {
                throw;
            }
        }
    }
}
