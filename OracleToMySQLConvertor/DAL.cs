using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OracleToMySQLConvertor
{
    public class DAL
    {
        public static DataTable GetTableList(Database db)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME LIKE 'LDB1_%' ORDER BY TABLE_NAME ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetTableColumns(Database db, string schemaName, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" select col.column_id, col.owner as schema_name,col.table_name,col.column_name,col.data_type,col.data_length,col.data_precision,col.data_scale,col.nullable,col.data_default ");
            sqlCmdBuilder.Append(" from all_tab_columns col ");
            sqlCmdBuilder.Append(" inner join all_tables t on col.owner = t.owner and col.table_name = t.table_name ");
            sqlCmdBuilder.Append(" where col.table_name = :TABLE_NAME and col.owner=:SCHEMA_NAME ");
            sqlCmdBuilder.Append(" order by col.column_id ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);
            db.AddInParameter(dbCmd, ":SCHEMA_NAME", DbType.AnsiString, schemaName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetPrimaryKeyDetails(Database db, string schemaName, string tableName, string constraintName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner,cons.constraint_name ");
            sqlCmdBuilder.Append(" FROM all_constraints cons, all_cons_columns cols ");
            sqlCmdBuilder.Append(" WHERE cons.constraint_type = 'P' ");
            sqlCmdBuilder.Append(" AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner and cons.owner=:SCHEMA_NAME ");

            if (tableName.Length > 0)
                sqlCmdBuilder.Append(" AND cols.table_name = :TABLE_NAME ");

            if (constraintName.Length > 0)
                sqlCmdBuilder.Append(" AND cons.constraint_name=:CONSTRAINT_NAME ");

            sqlCmdBuilder.Append(" ORDER BY cols.table_name, cols.position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":SCHEMA_NAME", DbType.AnsiString, schemaName);

            if (tableName.Length > 0)
                db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            if (constraintName.Length > 0)
                db.AddInParameter(dbCmd, ":CONSTRAINT_NAME", DbType.AnsiString, constraintName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetUniqueKeyDetails(Database db, string schemaName, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner,cons.constraint_name ");
            sqlCmdBuilder.Append(" FROM all_constraints cons, all_cons_columns cols ");
            sqlCmdBuilder.Append(" WHERE cols.table_name = :TABLE_NAME AND cons.constraint_type = 'U' ");
            sqlCmdBuilder.Append(" AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner and cons.owner=:SCHEMA_NAME ");
            sqlCmdBuilder.Append(" ORDER BY cols.table_name, cols.position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":SCHEMA_NAME", DbType.AnsiString, schemaName);
            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetForeignKeyDetails(Database db, string schemaName, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner,cons.constraint_name,cons.r_constraint_name ");
            sqlCmdBuilder.Append(" FROM all_constraints cons ");
            sqlCmdBuilder.Append(" INNER JOIN all_cons_columns cols on cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ");
            sqlCmdBuilder.Append(" WHERE cols.table_name = :TABLE_NAME AND cons.constraint_type = 'R' and cons.owner=:SCHEMA_NAME ");
            sqlCmdBuilder.Append(" ORDER BY cols.table_name, cols.position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":SCHEMA_NAME", DbType.AnsiString, schemaName);
            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetIndexDetails(Database db, string schemaName, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" select ind.index_name,ind_col.column_name,ind.index_type,ind.uniqueness,ind_col.column_position,ind.table_owner as schema_name,ind.table_name as object_name,ind.table_type as object_type,ind_exp.column_expression ");
            sqlCmdBuilder.Append(" from all_indexes ind ");
            sqlCmdBuilder.Append(" inner join all_ind_columns ind_col on ind.owner = ind_col.index_owner and ind.index_name = ind_col.index_name ");
            sqlCmdBuilder.Append(" left outer join sys.all_ind_expressions ind_exp on ind.owner = ind_exp.index_owner and ind.index_name = ind_exp.index_name and ind_col.column_position = ind_exp.column_position ");
            sqlCmdBuilder.Append(" where ind.owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS', 'LBACSYS', 'MDSYS', 'MGMT_VIEW', 'OLAPSYS', 'OWBSYS', 'ORDPLUGINS', 'ORDSYS', 'OUTLN','SI_INFORMTN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM', 'TSMSYS', 'WK_TEST',");
            sqlCmdBuilder.Append(" 'WKPROXY', 'WMSYS', 'XDB', 'APEX_040000', 'APEX_PUBLIC_USER', 'DIP', 'WKSYS',");
            sqlCmdBuilder.Append(" 'FLOWS_30000', 'FLOWS_FILES', 'MDDATA', 'ORACLE_OCM', 'XS$NULL',");//-- excluding some Oracle maintained schemas
            sqlCmdBuilder.Append(" 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'PUBLIC') ");
            sqlCmdBuilder.Append(" AND ind.table_name=:TABLE_NAME and ind.table_owner=:SCHEMA_NAME ");
            sqlCmdBuilder.Append(" order by ind.table_owner,ind.table_name,ind.index_name,ind_col.column_position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":SCHEMA_NAME", DbType.AnsiString, schemaName);
            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static string ParseQueryToMySQLDB(string expression)
        {
            #region DecodeConvertion
            string decodePattern = @"DECODE\([a-zA-z0-9.?:]*(,)NULL"; // Mysql does not support IF(FINSTRUCTION=NULL)=> So changing that into IF(FINSTRUCTION IS NULL)
            string queryWithRegexConvertion = Regex.Replace(expression, decodePattern, (_match) =>
            {
                Group group = _match.Groups[1];
                string replace = " IS ";
                string replaceNull = " OR " + _match.Value.Substring(0, group.Index - _match.Index).Replace("DECODE(", "") + " = ' '";
                return String.Format("{0}{1}{2}{3}", _match.Value.Substring(0, group.Index - _match.Index), replace, _match.Value.Substring(group.Index - _match.Index + group.Length), replaceNull);
            });

            decodePattern = @"DECODE\([a-zA-z0-9.?:]*(,)";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, decodePattern, (_match) =>
            {
                Group group = _match.Groups[1];
                string replace = "=";
                return String.Format("{0}{1}{2}", _match.Value.Substring(0, group.Index - _match.Index), replace, _match.Value.Substring(group.Index - _match.Index + group.Length));
            });

            //decodePattern = @"DECODE\([a-zA-z0-9.,'()]*\)(,)";
            decodePattern = @"DECODE\(NVL[a-zA-z0-9.,'(]*\)(,)";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, decodePattern, (_match) =>
            {
                Group group = _match.Groups[1];
                string replace = "=";
                return String.Format("{0}{1}{2}", _match.Value.Substring(0, group.Index - _match.Index), replace, _match.Value.Substring(group.Index - _match.Index + group.Length));
            });
            #endregion

            #region Number Convertion
            string numberPattern = @"\sNUMBER\([0-9]*,[0-9]\)";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, numberPattern, (_match) =>
            {
                string replace = " UNSIGNED";
                return String.Format("{0}", replace);
            });
            #endregion

            #region LTRIM Conversion
            decodePattern = @"LTRIM\([a-zA-z.]*(,)\'{0,1}.\'{0,1}\)";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, decodePattern, delegate (Match _match)
            {
                Group group = _match.Groups[1];
                string trimValue = _match.Value.Replace("LTRIM(", "").TrimEnd(')');
                return String.Format("TRIM(LEADING {0} FROM {1})", trimValue.Split(',')[1], trimValue.Split(',')[0]);
            });

            decodePattern = @"LTRIM\([a-zA-z0-9).,'()]*(,)\'{0,1}[a-zA-Z0-9.\s]*\'{0,1}\)(,)\'{0,1}.\'{0,1}\)";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, decodePattern, delegate (Match _match)
            {
                Group group = _match.Groups[1];
                string trimValue = _match.Value.Replace("LTRIM(", "");
                string extraStr = string.Empty;
                if (trimValue.Where(x => x == ')').Count() == 3)
                {
                    int ind = trimValue.IndexOf(")", trimValue.IndexOf(")") + 1);
                    extraStr = trimValue.Substring(ind + 1);
                    trimValue = trimValue.Substring(0, ind + 1);
                }
                trimValue = trimValue.TrimEnd(')');

                return String.Format("TRIM(LEADING {0} FROM {1})", trimValue.Split(',')[2], trimValue.Split(',')[0] + "," + trimValue.Split(',')[1]) + extraStr;
            });
            #endregion

            #region Date Subtration

            decodePattern = @"TO_CHAR\(SYSDATE-([a-zA-z0-9,']*\))";
            queryWithRegexConvertion = Regex.Replace(queryWithRegexConvertion, decodePattern, delegate (Match _match)
            {
                Group group = _match.Groups[1];
                return String.Format("DATE_FORMAT(DATE_SUB(SYSDATE(), INTERVAL {0} DAY),{1})", _match.Value.Substring(group.Index - _match.Index, group.ToString().IndexOf(",")),
                    _match.Value.Substring(group.Index - _match.Index + group.ToString().IndexOf("'"), (group.Length - (group.ToString().IndexOf("'") + 1))));
            });

            #endregion

            StringBuilder reqQuery = new StringBuilder();
            reqQuery.Append(queryWithRegexConvertion);
            reqQuery.Replace(":", "?").Replace("NVL(", "IFNULL(").Replace("DECODE(", "IF(").Replace("AND ROWNUM=", "LIMIT ").Replace("ROWNUM=", "LIMIT ").Replace("WHERE ROWNUM<=", "LIMIT ")
                .Replace("YYYYMMDD", "%Y%m%d").Replace("HH24MISS", "%H%i%s").Replace("HH24:MI:SS", "%H%i%s").Replace("TO_CHAR(TO_DATE", "DATE_FORMAT(TO_DATE").Replace("TO_DATE(", "STR_TO_DATE(").Replace("FETCH FIRST ROW ONLY", "LIMIT 1")
                .Replace("TO_CHAR(MAX(", "DATE_FORMAT(MAX(").Replace("TO_CHAR(MIN(", "DATE_FORMAT(MIN(").Replace("LISTAGG(", "GROUP_CONCAT(").Replace(",U'", ",'")
                .Replace("TO_NUMBER(TO_CHAR(SYSDATE,'yyyyMMdd'))", "DATE_FORMAT(SYSDATE(),'%Y%m%d')");

            return reqQuery.ToString();
        }

        public static bool ExecuteMySQLScript(Database db, string script)
        {
            DbCommand dbCmd = db.GetSqlStringCommand(script);
            dbCmd.CommandType = CommandType.Text;

            db.ExecuteNonQuery(dbCmd);
            return true;
        }
    }
}
