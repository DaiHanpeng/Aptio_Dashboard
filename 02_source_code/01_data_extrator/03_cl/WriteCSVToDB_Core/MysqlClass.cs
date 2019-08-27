using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;
using System.IO;
using System.Threading;
using System.Configuration;

namespace WriteCSVToDB_Core
{
    static class MysqlClass
    {
        //public static string connectstring = "server=192.168.1.252;user id=fse;password=inpeco!it;database=DMSCN";
        public static string DBIP, DBUserName, DBPassword,DataBaseName;
        public static string DBConnectString = "";
        public static MySqlCommand dbcommand;

        public static MySqlConnection ConnectDb(string DBProfile = "")
        {
            MySqlConnection dbconnect;
            if (DBProfile == "")
            {
                DBProfile = DBConnectString;
            }
            try
            {
                dbconnect = new MySqlConnection(DBProfile);
                dbconnect.Open();
            }
            catch (Exception e)
            {
                Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                return null;
            }
            

            return dbconnect;
        }

        public static void InitDatabasePara(string dbname = "")
        {
            DBIP = ConfigurationManager.AppSettings["DBIP"];
            DBUserName = ConfigurationManager.AppSettings["DBUser"];
            DBPassword = ConfigurationManager.AppSettings["DBPassword"];
            if (dbname != "")
            {
                DataBaseName = dbname;
            }
            else
            {
                DataBaseName = ConfigurationManager.AppSettings["DBDefaultName"];
            }
            DBConnectString = "server=" + DBIP + ";user id="+ DBUserName + ";password="+ DBPassword + ";database=" + DataBaseName;


        }

        public static MySqlConnection GetNewConnect(string DBProfile = "")
        {
            MySqlConnection dbnconnect;
            Boolean success = false;
            int timeout = 0;
            if (DBProfile == "")
            {
                DBProfile = DBConnectString;
            }
            while (!success)
            {
                try
                {


                    dbnconnect = new MySqlConnection(DBProfile);

                    dbnconnect.Open();
                    //dbnconnect.ChangeDatabase(DBname);
                    success = true;
                    return dbnconnect;
                }
                catch (Exception e)
                {

                    timeout++;
                    if (timeout > 10)
                    {
                        Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                        return null;
                    }


                }
            }
            return null;


        }

        public static DataTable GetDataTable(string sql, string tablename, string DBProfile = "", Boolean ShowErrorMessage = false)
        {
            if (DBProfile == "")
            {
                DBProfile = DBConnectString;
            }
            MySqlConnection MSC = GetNewConnect(DBProfile);
            DataTable dt = new DataTable(tablename);//= new DataTable(tablename);
            DataSet ds = new DataSet(tablename);
            Boolean success = false;
            int trycycle = 0;
            while (!success)
            {
                try
                {
                    if (MSC == null)
                    {
                        MSC = GetNewConnect(DBProfile);
                    }
                    MySqlDataAdapter MSDA = new MySqlDataAdapter(sql, MSC);
                    int i = MSDA.Fill(ds, tablename);
                    dt = ds.Tables[tablename];
                    success = true;
                    ds.Dispose();
                    MSC.Close();
                    MSC.Dispose();
                    MSDA.Dispose();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    if (ShowErrorMessage)
                    {
                        Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                    }
                    trycycle++;
                    if (trycycle > 10)
                    {
                        Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                        Thread.Sleep(5);
                        //if (MSC != null)
                        //{
                        //    MSC.Close();
                        //    MSC.Dispose();
                        //}
                        return dt;
                    }
                }
            }




            return dt;
        }

        public static bool GetColNameType(string databasena,string TableName,ref Dictionary<string,string> tabletype)
        {
            MySqlCommand cmd = null;
            MySqlDataReader reader = null;
            List<string> list_ColName = new List<string>();
            List<Type> list_ColType = new List<Type>();
            string sql = "show columns from "+ databasena + "." + TableName + " ;";
            MySqlConnection conn = ConnectDb(DBConnectString);
            if (conn == null)
            {
                return false;
            }
            cmd = new MySqlCommand(sql, conn);
            try
            {
                reader = cmd.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        string t = reader.GetString(0);
                        string tt = reader.GetValue(1).ToString();
                        tabletype.Add(t, tt);
                    }
                }
                reader.Close();
                conn.Close();
                return true;
            }
            catch (Exception e)
            {
                Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                return false;
            }
        }

        public static int ExecuteSQL(string sql, string DBProfile = "", Boolean ShowErrormessage = false)
        {
            MySqlConnection MSC = GetNewConnect(DBProfile); ;
            Boolean success = false;
            int trycycle = 0;
            while (!success)
            {
                try
                {
                    if (MSC == null)
                    {
                        MSC = GetNewConnect(DBProfile);
                    }

                    dbcommand = new MySqlCommand(sql, MSC);
                    dbcommand.ExecuteNonQuery();
                    success = true;
                    MSC.Close();
                    MSC.Dispose();
                    //dbcommand.BeginExecuteNonQuery
                }
                catch (Exception e)
                {
                    if (ShowErrormessage)
                    {
                        Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                    }

                    trycycle++;
                    if (trycycle > 10)
                    {
                        Logging.WriteLog("SQLERROR", e.Message + "\r\n");
                        //if (MSC!=null)
                        //{
                        //    MSC.Close();
                        //    MSC.Dispose();
                        //}

                        return -1;
                    }


                }

            }


            return 0;
        }

    }
}
