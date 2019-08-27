using System;
using System.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace WriteCSVToDB_Core
{
    static class CSVToDB
    {
        public static Dictionary<string, DataTable> ddt;
        public static void Process(string logpath,string RecordID,string TargetDBName)
        {
            try
            {
                string filelist = ConfigurationManager.AppSettings["LoadList"];
                string[] files = filelist.Split(',');
                if (Tools.CheckFileList(logpath, filelist) < 0)
                {
                    Logging.WriteLog("SystemError", "Missing necessary csv data files");
                    StatusRefresh.UpdateStatus(RecordID, "-1", "0/" + files.Length.ToString(), "Missing necessary csv data files");
                    return;
                }
                Dictionary<string, DataTable> csvtables = Tools.GetDatatable(logpath, filelist);
                ddt = csvtables;
                int count = 0;
                foreach(KeyValuePair<string,DataTable> keyValue in csvtables)
                {
                    string tablename = keyValue.Key;
                    DataTable data = keyValue.Value;
                    //if (tablename != "cl_flag")
                    //{
                    CleanDB(TargetDBName, tablename);
                    Logging.WriteLog("TableProcessing", "Process " + tablename + " Start");
                    BulkWriteRecordToDB(tablename, data, TargetDBName);

                    
                    //}
                    count++;
                    StatusRefresh.UpdateStatus(RecordID, "1", count.ToString() + "/" + files.Length.ToString(), "Process " + tablename + " complete!");

                }
                ddt.Clear();
                GC.Collect();

                StatusRefresh.UpdateStatus(RecordID, "2", count.ToString() + "/" + files.Length.ToString(), "Process Done!");
            }
            catch(Exception ex)
            {
                Logging.WriteLog("SystemError in Class CSVToDB", ex.Message);
                StatusRefresh.UpdateStatus(RecordID, "-1", "", ex.Message);
                return;
            }
        }
        
        private static void WriteRecordToDB(string tablename,DataTable dt,string TargetDBName)
        {
            try
            {
                Dictionary<string, string> tabletype = new Dictionary<string, string>();
                MysqlClass.GetColNameType(TargetDBName, tablename, ref tabletype);
                if (tabletype.Count == 0)
                {
                    return;
                }

                foreach (DataRow row in dt.Rows)
                {
                    string sql = "insert into " + TargetDBName + "." + tablename + "(";
                    string columnslist = "";
                    string valuelist = "";
                    string value;
                    foreach (DataColumn c in dt.Columns)
                    {
                        if (tabletype.ContainsKey(c.ColumnName))
                        {
                            columnslist += c.ColumnName + ",";
                            //

                            if (tabletype[c.ColumnName].ToUpper().IndexOf("INT") >= 0 || tabletype[c.ColumnName].ToUpper().IndexOf("DEC") >= 0)
                            {
                                value = row[c.ColumnName].ToString();
                                if (value == "?")
                                {
                                    valuelist += "null,";
                                }
                                else
                                {
                                    valuelist += value + ",";
                                }
                                continue;
                            }
                            //char,varchar
                            if (tabletype[c.ColumnName].ToUpper().IndexOf("CHAR") >= 0)
                            {
                                value = row[c.ColumnName].ToString();
                                if (value == "?")
                                {
                                    valuelist += "null,";
                                }
                                else
                                {
                                    valuelist += "'" + value + "'" + ",";
                                }
                                continue;
                            }
                            //datetime
                            if (tabletype[c.ColumnName].ToUpper() == "DATATIME")
                            {
                                value = row[c.ColumnName].ToString();
                                if (value == "?")
                                {
                                    valuelist += "null,";
                                }
                                else
                                {
                                    try
                                    {
                                        var datetime = DateTime.Parse(value);
                                        value = datetime.ToLongDateString() + " " + datetime.ToLongTimeString();
                                    }
                                    catch
                                    {
                                        value = "null";
                                        continue;
                                    }
                                    valuelist += "'" + value + "'" + ",";


                                }
                                continue;
                            }
                            //date
                            if (tabletype[c.ColumnName].ToUpper() == "DATA")
                            {
                                value = row[c.ColumnName].ToString();

                                if (value == "?")
                                {
                                    valuelist += "null,";
                                }
                                else
                                {
                                    try
                                    {
                                        var datetime = DateTime.Parse(value);
                                        value = datetime.ToLongDateString();
                                    }
                                    catch
                                    {
                                        valuelist += "null,";
                                        continue;
                                    }
                                    valuelist += "'" + row[c.ColumnName].ToString() + "'" + ",";
                                }
                                continue;
                            }

                        }


                    }
                    columnslist = columnslist.Substring(0, columnslist.Length - 1);
                    valuelist = valuelist.Substring(0, valuelist.Length - 1);
                    string excutesql = sql + columnslist + ") values(" + valuelist + ")";
                    MysqlClass.ExecuteSQL(excutesql);
                    //if (tablename.ToUpper() == "cl_result".ToUpper())
                    //{
                    //    string recordid = GetResultID(row);
                    //}
                }
            }
            catch(Exception ex)
            {
                Logging.WriteLog("SystemError in Class CSVToDB", ex.Message);
            }
        }
        private static void BulkWriteRecordToDB(string tablename, DataTable dt, string TargetDBName)
        {
            try
            {
                Dictionary<string, string> tabletype = new Dictionary<string, string>();
                MysqlClass.GetColNameType(TargetDBName, tablename, ref tabletype);
                if (tabletype.Count == 0)
                {
                    return;
                }




                StringBuilder sqlbuilder = new StringBuilder() ;
                sqlbuilder.Append("insert into " + TargetDBName + "." + tablename + "(");
                string sqlcolumns = "";
                string columnnamelist = "";
                string[] columnarry;
                foreach (DataColumn c in dt.Columns)
                {
                    if (tabletype.ContainsKey(c.ColumnName))
                    {
                        sqlcolumns += "`" + c.ColumnName+ "`" + ",";
                        columnnamelist += c.ColumnName + ",";


                    }

                }
                sqlcolumns = sqlcolumns.Substring(0, sqlcolumns.Length - 1);
                columnnamelist = columnnamelist.Substring(0, columnnamelist.Length - 1);
                columnarry = columnnamelist.Split(',');
                sqlbuilder.Append(sqlcolumns + ") ");

                if (columnarry.Length > 0)
                {
                    StringBuilder sbvalues = new StringBuilder();
                    sbvalues.Append(" value ");
                    if (dt.Rows.Count > 0)
                    {
                        Logging.WriteLog("TableProcessing ", " Process " + tablename + "  " +  dt.Rows.Count.ToString() + " "  + " Rows");
                        foreach (DataRow row in dt.Rows)
                        {

                            StringBuilder sbsinglevalus = new StringBuilder();
                            sbsinglevalus.Append("(");

                            foreach (string colna in columnarry)
                            {

                                string value;
                                if (tabletype[colna].ToUpper().IndexOf("INT") >= 0 || tabletype[colna].ToUpper().IndexOf("DEC") >= 0)
                                {
                                    value = row[colna].ToString();
                                    if (value == "?")
                                    {
                                        sbsinglevalus.Append("null,");
                                    }
                                    else
                                    {
                                        sbsinglevalus.Append(value + ",");
                                    }
                                    continue;
                                }
                                //char,varchar
                                if (tabletype[colna].ToUpper().IndexOf("CHAR") >= 0)
                                {
                                    value = row[colna].ToString();
                                    if (value == "?")
                                    {
                                        sbsinglevalus.Append("null,");
                                    }
                                    else
                                    {
                                        sbsinglevalus.Append("'" + value + "'" + ",");
                                    }
                                    continue;
                                }
                                //datetime
                                if (tabletype[colna].ToUpper() == "DATETIME")
                                {
                                    value = row[colna].ToString();
                                    if (value == "?")
                                    {
                                        sbsinglevalus.Append("null,");
                                    }
                                    else
                                    {
                                        //try
                                        //{
                                        //    var datetime = DateTime.Parse(value);
                                        //    value = datetime.ToLongDateString() + " " + datetime.ToLongTimeString();
                                        //}
                                        //catch
                                        //{
                                        //    value = "null";
                                        //    continue;
                                        //}
                                        sbsinglevalus.Append("'" + value + "'" + ",");


                                    }
                                    continue;
                                }
                                //date
                                if (tabletype[colna].ToUpper() == "DATE")
                                {
                                    value = row[colna].ToString();

                                    if (value == "?")
                                    {
                                        sbsinglevalus.Append("null,");
                                    }
                                    else
                                    {
                                        //try
                                        //{
                                        //    var datetime = DateTime.Parse(value);
                                        //    value = datetime.ToLongDateString();
                                        //}
                                        //catch
                                        //{
                                        //    sbsinglevalus.Append("null,");
                                        //    continue;
                                        //}
                                        sbsinglevalus.Append("'" + value + "'" + ",");
                                    }
                                    continue;
                                }

                            }
                            string ssinglevalue = sbsinglevalus.ToString();
                            ssinglevalue = ssinglevalue.Substring(0, ssinglevalue.Length - 1) + "),";
                            sbvalues.Append(ssinglevalue);

                            if (sbvalues.Length > 300000)
                            {
                                string sqlvalues = sbvalues.ToString();
                                sbvalues = new StringBuilder();
                                sbvalues.Append(" value ");
                                sqlvalues = sqlvalues.Substring(0, sqlvalues.Length - 1);
                                StringBuilder sbtemp = new StringBuilder();
                                sbtemp.Append(sqlbuilder.ToString() + sqlvalues);
                                string sqltemp = sbtemp.ToString();
                                MysqlClass.ExecuteSQL(sqltemp);
                            }



                        }

                        string sqlvalue = sbvalues.ToString();
                        sqlvalue = sqlvalue.Substring(0, sqlvalue.Length - 1);

                        sqlbuilder.Append(sqlvalue);

                        string excutesql = sqlbuilder.ToString();
                        MysqlClass.ExecuteSQL(excutesql);
                    }
                }
                


            }
            catch (Exception ex)
            {
                Logging.WriteLog("SystemError in Class CSVToDB", ex.Message);
                Logging.WriteLog("TableProcessing", "Process " + tablename + " Error");
            }
            Logging.WriteLog("TableProcessing", "Process " + tablename + " Done");
            ddt[tablename].Clear();
            GC.Collect();
        }

        


        private static void CleanDB(string targetdbname,string tablename)
        {
            string sql = "delete from " + targetdbname + "." + tablename;
            MysqlClass.ExecuteSQL(sql);
        }
        
    }
}
