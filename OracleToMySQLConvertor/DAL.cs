using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OracleToMySQLConvertor
{
    public class DAL
    {
        public static DataTable GetTableList(Database db)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT TABLE_NAME FROM USER_TABLES WHERE ORDER BY TABLE_NAME ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetTableColumns(Database db, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" select col.column_id, col.owner as schema_name,col.table_name,col.column_name,col.data_type,col.data_length,col.data_precision,col.data_scale,col.nullable,col.data_default ");
            sqlCmdBuilder.Append(" from all_tab_columns col ");
            sqlCmdBuilder.Append(" inner join all_tables t on col.owner = t.owner and col.table_name = t.table_name ");
            sqlCmdBuilder.Append(" where col.table_name = :TABLE_NAME ");
            sqlCmdBuilder.Append(" order by col.column_id ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetPrimaryKeyDetails(Database db, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner,cons.constraint_name ");
            sqlCmdBuilder.Append(" FROM all_constraints cons, all_cons_columns cols ");
            sqlCmdBuilder.Append(" WHERE cols.table_name = :TABLE_NAME AND cons.constraint_type = 'P' ");
            sqlCmdBuilder.Append(" AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ");
            sqlCmdBuilder.Append(" ORDER BY cols.table_name, cols.position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetUniqueKeyDetails(Database db, string tableName)
        {
            StringBuilder sqlCmdBuilder = new StringBuilder();
            sqlCmdBuilder.Append(" SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner,cons.constraint_name ");
            sqlCmdBuilder.Append(" FROM all_constraints cons, all_cons_columns cols ");
            sqlCmdBuilder.Append(" WHERE cols.table_name = :TABLE_NAME AND cons.constraint_type = 'U' ");
            sqlCmdBuilder.Append(" AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ");
            sqlCmdBuilder.Append(" ORDER BY cols.table_name, cols.position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }

        public static DataTable GetIndexDetails(Database db, string tableName)
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
            sqlCmdBuilder.Append(" AND ind.table_name=:TABLE_NAME ");
            sqlCmdBuilder.Append(" order by ind.table_owner,ind.table_name,ind.index_name,ind_col.column_position ");

            DbCommand dbCmd = db.GetSqlStringCommand(sqlCmdBuilder.ToString());
            dbCmd.CommandType = CommandType.Text;

            db.AddInParameter(dbCmd, ":TABLE_NAME", DbType.AnsiString, tableName);

            return db.ExecuteDataSet(dbCmd).Tables[0];
        }
    }
}
