using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OracleToMySQLConvertor
{
    class Program
    {
        static string FolderPath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\Scripts";
        static string LogFilePath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "\\log_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt";
        static bool ImportToMySQLServer = ConfigurationManager.AppSettings["ImportSchemaToMySQLServer"].ToString().ToUpper() == "TRUE" ? true : false;

        static int TableIndex = 0;
        static int TableCount = 0;
        static List<string> CompletedTables = new List<string>();

        static void Main(string[] args)
        {
            try
            {
                if (!Directory.Exists(FolderPath))
                    Directory.CreateDirectory(FolderPath);

                Database db = DatabaseFactory.CreateDatabase("OracleConnection");
                Database mySQLDB = DatabaseFactory.CreateDatabase("MySQLConnection");
                DataTable tableList = DAL.GetTableList(db);
                TableCount = tableList.Rows.Count;

                foreach(DataRow tableRow in tableList.Rows)
                {
                    string tableName = tableRow["TABLE_NAME"].ToString().ToUpper();
                    if (!CompletedTables.Contains(tableName))
                        ConvertTable(db, mySQLDB, tableName);
                }

                Console.WriteLine("Completed.");
                WriteLog("Completed.");
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

        static void WriteLog(string content)
        {
            System.IO.File.AppendAllText(LogFilePath, content + "\n");
        }

        static void ConvertTable(Database db, Database mySQLDB, string tableName)
        {
            string logDesc = string.Format("Converting Table {0} ({1}/{2})...", tableName, ++TableIndex, TableCount);
            Console.WriteLine(logDesc);
            WriteLog(logDesc);

            DataTable columnTable = DAL.GetTableColumns(db, tableName);
            DataTable primaryKeyTable = DAL.GetPrimaryKeyDetails(db, tableName, string.Empty);
            DataTable uniqueKeyTable = DAL.GetUniqueKeyDetails(db, tableName);
            DataTable foreignKeyTable = DAL.GetForeignKeyDetails(db, tableName);
            DataTable indexTable = DAL.GetIndexDetails(db, tableName);

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

                if (columnName.Contains("FTIMESTAMP"))
                    schemaBuilder.AppendFormat("{0}\t TIMESTAMP", columnName);
                else if (dataType == "VARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tVARCHAR({1})", columnName, dataLength);
                else if (dataType == "NVARCHAR2")
                    schemaBuilder.AppendFormat("{0}\tNVARCHAR({1})", columnName, dataLength / 2);//in oracle it doubles the datalength of NVARCHAR2
                else if (dataType == "CHAR")
                    schemaBuilder.AppendFormat("{0}\tCHAR({1})", columnName, dataLength);
                else if (dataType == "NUMBER" && dataScale == 0)
                    schemaBuilder.AppendFormat("{0}\tINT", columnName);
                else if (dataType == "NUMBER" && dataScale > 0)
                    schemaBuilder.AppendFormat("{0}\tDECIMAL({1},{2})", columnName, dataPrecision, dataScale);
                else if (dataType == "NCLOB")
                    schemaBuilder.AppendFormat("{0}\tMEDIUMTEXT", columnName);
                else
                    schemaBuilder.AppendFormat("{0}\t{1}", columnName, dataType);

                if (nullable == "N")
                    schemaBuilder.Append("\tNOT NULL");
                else
                {
                    if (columnName.Contains("FTIMESTAMP"))
                        schemaBuilder.AppendFormat("\tDEFAULT CURRENT_TIMESTAMP");
                    else if (dataDefault.Contains("TO_CHAR") || dataDefault.Contains("SYSDATE"))
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", "(" + DAL.ParseQueryToMySQLDB(dataDefault) + ")");
                    else if(dataDefault.Contains("--"))
                        schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault.Substring(0, dataDefault.IndexOf("--")).Trim());
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
                    string rTableName = string.Empty;
                    string rContraintName = string.Empty;
                    List<string> columnList = new List<string>();

                    foreach (DataRow row in uniqueKeyColumns)
                    {
                        rTableName = row["R_TABLE_NAME"].ToString().ToUpper();
                        rContraintName = row["R_CONSTRAINT_NAME"].ToString().ToUpper();
                        string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                        columnList.Add(columnName);
                    }

                    List<string> rColumnList = new List<string>();
                    DataTable primaryKeyInfoTable = DAL.GetPrimaryKeyDetails(db, rTableName, rContraintName);
                    foreach(DataRow row in primaryKeyInfoTable.Rows)
                    {
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
                    Console.WriteLine("Executing Foreign key table " + forignkeyTable);
                    WriteLog("Executing Foreign key table " + forignkeyTable);
                    ConvertTable(db, mySQLDB, forignkeyTable);
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
                                if (colExp.Contains("NVL") || colExp.Contains("DECODE") || colExp.Contains("UPPER") || colExp.Contains("TO_CHAR") || colExp.Contains("TO_DATE"))
                                    modifiedExpList.Add("(" + DAL.ParseQueryToMySQLDB(colExp) + ")");
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
            if (ImportToMySQLServer)
            {
                var queries = schemaBuilder.ToString().Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                foreach (string query in queries)
                {
                    try
                    {
                        DAL.ExecuteMySQLScript(mySQLDB, query);
                        schemaLogBuilder.Append(query + ";");
                    }
                    catch(Exception ex)
                    {
                        schemaLogBuilder.Append(query + ";");
                        schemaLogBuilder.Append("\n-- Exception: " + ex.Message);
                        Console.WriteLine(tableName + ": " + ex.Message);
                        WriteLog(tableName + ": " + ex.Message);
                    }
                }
            }
            else
            {
                schemaLogBuilder.Append(schemaBuilder.ToString());
            }

            //Write table schema into file.
            WriteToFile(tableName, schemaLogBuilder.ToString());

            CompletedTables.Add(tableName);
        }
    }
}
