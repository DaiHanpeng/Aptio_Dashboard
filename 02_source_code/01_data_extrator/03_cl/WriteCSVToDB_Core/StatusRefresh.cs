using System;
using System.Configuration;
using System.Data;
using System.Collections.Generic;
using System.Text;

namespace WriteCSVToDB_Core
{
    static class StatusRefresh
    {
        public static string connectstring = "server=" + ConfigurationManager.AppSettings["DBIP"] + ";user id=" + ConfigurationManager.AppSettings["DBUser"] + ";password=" + ConfigurationManager.AppSettings["DBPassword"] + ";database=datacake";
        public static Boolean CheckStatusDBConnect(string RecordID)
        {
            try
            {
                if (MysqlClass.ConnectDb(connectstring) == null)
                {
                    return false;
                }
                string sql = "select id,ToCLStatus, ToCLProcess, ToCLRemark from datacake.sync_datasource where id = " + RecordID;
                DataTable dt =  MysqlClass.GetDataTable(sql, "recordtable", connectstring);
                if (dt.Rows.Count > 0)
                {
                    return true;
                }

            }
            catch(Exception e)
            {
                Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                return false;
            }
            return false;
        }

        public static void UpdateStatus(string RecordID,string status  = "",string process = "",string remark = "")
        {
            string sql;
            if (!Program.CanUpdateStatus)
            {
                Console.WriteLine(process + "   " + remark);
                return;
            }

            try
            {
                if (process != "")
                {
                    sql = "update datacake.sync_datasource set ToCLStatus = " + status + " where id = " + RecordID;
                    MysqlClass.ExecuteSQL(sql, connectstring);
                }
                if (status != "")
                {
                    sql = "update datacake.sync_datasource set ToCLProcess = '" + process + "' where id = " + RecordID;
                    MysqlClass.ExecuteSQL(sql, connectstring);
                }
                if (remark != "")
                {
                    sql = "update datacake.sync_datasource set ToCLRemark = '" + remark + "' where id = " + RecordID;
                    MysqlClass.ExecuteSQL(sql, connectstring);
                }
            }
            catch (Exception e)
            {
                Logging.WriteLog("SQLERROR", e.Message + "\r\n");
            }
        }
        
    }
}
