using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OracleToMySQLConvertor
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Database db = DatabaseFactory.CreateDatabase("ApplicationConnection");
                DataTable tableList = DAL.GetTableList(db);

                foreach(DataRow tableRow in tableList.Rows)
                {
                    string tableName = tableRow["TABLE_NAME"].ToString().ToUpper();
                    Console.WriteLine("Converting Table " + tableName + "...");
                    DataTable columnTable = DAL.GetTableColumns(db, tableName);
                    DataTable primaryKeyTable = DAL.GetPrimaryKeyDetails(db, tableName);
                    DataTable uniqueKeyTable = DAL.GetUniqueKeyDetails(db, tableName);
                    DataTable indexTable = DAL.GetIndexDetails(db, tableName);

                    StringBuilder schemaBuilder = new StringBuilder();
                    schemaBuilder.AppendFormat("CREATE TABLE {0} \n(\n", tableName);
                    int colIndex = 0;
                    foreach(DataRow colRow in columnTable.Rows)
                    {
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

                        if(colIndex > 0)
                            schemaBuilder.Append(",");

                        if (dataType == "VARCHAR2")
                            schemaBuilder.AppendFormat("{0}\tVARCHAR({1})", columnName, dataLength);
                        else if(dataType == "NVARCHAR2")
                            schemaBuilder.AppendFormat("{0}\tNVARCHAR({1})", columnName, dataLength);
                        else if (dataType == "CHAR")
                            schemaBuilder.AppendFormat("{0}\tCHAR({1})", columnName, dataLength);
                        else if (dataType == "NUMBER" && dataScale == 0)
                            schemaBuilder.AppendFormat("{0}\tINT", columnName);
                        else if (dataType == "NUMBER" && dataScale > 0)
                            schemaBuilder.AppendFormat("{0}\tDECIMAL({1},{2})", columnName, dataPrecision, dataScale);
                        else if (dataType.Contains("TIMESTAMP"))
                            schemaBuilder.AppendFormat("{0}\t{1}", columnName, dataType);

                        if (nullable == "N")
                            schemaBuilder.Append("\tNOT NULL");
                        else
                            schemaBuilder.AppendFormat("\tDEFAULT {0}", dataDefault);                            

                        //add new line for each column
                        schemaBuilder.Append("\n");
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
                        schemaBuilder.AppendFormat(",CONSTRAINT {0} PRIMARY KEY ({1})\n", constraintName, string.Join(",", columnList));
                        constraintList.Add(constraintName);
                    }

                    if (uniqueKeyTable.Rows.Count > 0)
                    {
                        DataTable uniqueKeys = uniqueKeyTable.DefaultView.ToTable(true, "CONSTRAINT_NAME");
                        foreach(DataRow uniqueKeyRow in uniqueKeys.Rows)
                        {
                            string constraintName = uniqueKeyRow["CONSTRAINT_NAME"].ToString();
                            DataRow[] uniqueKeyColumns = uniqueKeyTable.Select("CONSTRAINT_NAME='" + constraintName + "'", "POSITION ASC");
                            List<string> columnList = new List<string>();
                            foreach (DataRow row in uniqueKeyColumns)
                            {
                                string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                                columnList.Add(columnName);
                            }
                            schemaBuilder.AppendFormat(",CONSTRAINT {0} UNIQUE KEY ({1})\n", constraintName, string.Join(",", columnList));
                            constraintList.Add(constraintName);
                        }
                    }

                    schemaBuilder.Append(");");

                    if (indexTable.Rows.Count > 0)
                    {
                        DataTable uniqueIndexs = indexTable.DefaultView.ToTable(true, "INDEX_NAME");
                        foreach (DataRow uniqueIndexRow in uniqueIndexs.Rows)
                        {
                            string indexName = uniqueIndexRow["INDEX_NAME"].ToString().ToUpper();
                            if (!constraintList.Contains(indexName))
                            {
                                DataRow[] indexColumns = indexTable.Select("INDEX_NAME='" + indexName + "'", "COLUMN_POSITION ASC");
                                string indexType = string.Empty;
                                string columnExpression = string.Empty;
                                List<string> columnList = new List<string>();
                                foreach (DataRow row in indexColumns)
                                {
                                    indexType = row["INDEX_TYPE"].ToString().ToUpper();
                                    columnExpression = row["COLUMN_EXPRESSION"].ToString().ToUpper();
                                    
                                    string columnName = row["COLUMN_NAME"].ToString().ToUpper();
                                    columnList.Add(columnName);
                                }

                                if (indexType != "FUNCTION-BASED NORMAL")
                                    schemaBuilder.AppendFormat("\nCREATE INDEX {0} ON {1}({2});", indexName, tableName, string.Join(",", columnList));
                                else
                                    schemaBuilder.AppendFormat("\nCREATE INDEX {0} ON {1}({2});", indexName, tableName, columnExpression);
                            }
                        }
                    }

                    WriteToFile(tableName, schemaBuilder.ToString());
                }

                Console.WriteLine("Completed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.ReadLine();
        }

        static void WriteToFile(string tableName, string content)
        {
            string fileName = "D:\\MySQL_DDL\\" + tableName + ".txt";
            System.IO.File.WriteAllText(fileName, content);
        }
    }
}
