using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;

namespace WriteCSVToDB_Core
{
    public static class Tools
    {
        public static DataTable CSVToDatatable(string csvfile, string datatabelname)
        {
            Encoding encoding = Encoding.ASCII;
            DataTable dt = new DataTable();
            dt.TableName = datatabelname;
            FileStream fs = new FileStream(csvfile, FileMode.Open, FileAccess.Read);

            //StreamReader sr = new StreamReader(fs, Encoding.UTF8);
            StreamReader sr = new StreamReader(fs, encoding);
            //string fileContent = sr.ReadToEnd();
            //encoding = sr.CurrentEncoding;
            //记录每次读取的一行记录
            string strLine = "";
            //记录每行记录中的各字段内容
            string[] aryLine = null;
            string[] tableHead = null;
            //标示列数
            int columnCount = 0;
            //标示是否是读取的第一行
            bool IsFirst = true;
            //逐行读取CSV中的数据
            while ((strLine = sr.ReadLine()) != null)
            {
                //strLine = Common.ConvertStringUTF8(strLine, encoding);
                //strLine = Common.ConvertStringUTF8(strLine);
                if (IsFirst == true)
                {
                    strLine = strLine.Replace(".", "_");
                    strLine = strLine.Replace("~", "_");
                    tableHead = strLine.Split(',');
                    IsFirst = false;
                    columnCount = tableHead.Length;
                    //创建列
                    for (int i = 0; i < columnCount; i++)
                    {
                        DataColumn dc = new DataColumn(tableHead[i]);
                        dt.Columns.Add(dc);
                    }
                }
                else
                {
                    if (strLine.IndexOf('"') >= 0)
                    {
                        int startpos = strLine.IndexOf('"');
                        int endpos = strLine.IndexOf('"', startpos + 1);
                        while (endpos < 0)
                        {
                            strLine += sr.ReadLine();
                            startpos = strLine.IndexOf('"');
                            endpos = strLine.IndexOf('"', startpos + 1);
                        }
                        string temp = strLine.Substring(startpos, endpos - startpos);
                        strLine = temp.Replace(",", ";") + strLine.Substring(endpos + 1);
                    }
                    aryLine = strLine.Split(',');
                    if (aryLine.Length <= 1) continue;
                    if (aryLine.Length < columnCount) continue;
                    DataRow dr = dt.NewRow();

                    for (int j = 0; j < columnCount; j++)
                    {
                        dr[j] = aryLine[j];
                    }
                    dt.Rows.Add(dr);
                }
            }
            if (aryLine != null && aryLine.Length > 0)
            {
                dt.DefaultView.Sort = tableHead[0] + " " + "asc";
            }

            sr.Close();
            fs.Close();
            return dt;
        }

        public static void DataTableToCSV(DataTable dt, string FullPath)
        {
            string CSVSepStr1 = ",";
            FileInfo fi = new FileInfo(FullPath);
            if (!fi.Directory.Exists)
            {
                fi.Directory.Create();
            }
            FileStream fs = new FileStream(FullPath, System.IO.FileMode.Create, System.IO.FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.UTF8);
            string data = "";
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                data += dt.Columns[i].ColumnName.ToString();
                if (i < dt.Columns.Count - 1)
                {
                    data += CSVSepStr1;
                }
            }
            sw.WriteLine(data);


            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data = "";
                for (int j = 0; j < dt.Columns.Count; j++)
                {
                    string str = dt.Rows[i][j].ToString();
                    str = str.Replace("\"", "\"\"");//替换英文冒号 英文冒号需要换成两个冒号
                    if (str.Contains(',') || str.Contains('"')
                    || str.Contains('\r') || str.Contains('\n')) //含逗号 冒号 换行符的需要放到引号中
                    {
                        str = string.Format("\"{0}\"", str);
                    }
                    data += str;
                    if (j < dt.Columns.Count - 1)
                    {
                        data += CSVSepStr1;
                    }
                }
                sw.WriteLine(data);
            }
            sw.Close();
            fs.Close();
            //DialogResult result = MessageBox.Show("CSV文件保存成功！");
            //if (result == DialogResult.OK)
            //{
            //    System.Diagnostics.Process.Start("explorer.exe", Common.PATH_LANG);
            //}
        }
        
        public static int CheckFileList(string folderpath,string filelist)
        {
            var files = filelist.Split(',');
            foreach (string filename in files)
            {
                FileInfo fi = new FileInfo(folderpath + "\\" + filename);
                if (!fi.Exists)
                {
                    return -1;
                }

            }
            return 0;
        }
        public static Dictionary<string,DataTable> GetDatatable(string foldpath,string filelist)
        {
            Dictionary<string, DataTable> ddt = new Dictionary<string, DataTable>();
            string[] filelists = filelist.Split(',');
            foreach(string filename in filelists)
            {
                string tablena = filename.Substring(0, filename.IndexOf("."));
                DataTable dt = CSVToDatatable(foldpath + "\\" + filename, tablena);
                ddt.Add(tablena, dt);
            }
            return ddt;
        }
    }
}
