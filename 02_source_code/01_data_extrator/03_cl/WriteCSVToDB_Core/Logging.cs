using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;

namespace WriteCSVToDB_Core
{
    static class Logging
    {
        public static string LogPath, LogFile,DateDays;
        public static List<string[]> LogProcessList = new List<string[]>();
        public static Boolean ListLock = false;
        public static Boolean ThreadStop;
        public static int BWriteLog(string LogType,string LogText,string logtime,string logfile = "")
        {
            if (CheckDateChange())
            {
                LogPath = "";
            }
            if (LogPath == "" || LogPath == null)
            {
                InitLogFile(AppDomain.CurrentDomain.BaseDirectory + "\\" + DateTime.Now.ToString("yyyyMMdd") + "\\");
            }
            if (logfile == ""|| logfile is null)
            {
                logfile = LogFile;
            }
            try
            {
                
                string text = logtime + "    " + LogType + " : " + LogText;
                StreamWriter sw = new StreamWriter(logfile, true);
                sw.WriteLine(text);
                sw.Close();
                
                
                
            }
            catch (Exception e)
            {
                System.Console.WriteLine("LogWriter" + e.Message);
                WriteLog("SystemError", "LogWriter" + e.Message);
                return -1;
            }

            return 0;
        }
        public static Boolean CheckDateChange()
        {
            if (string.IsNullOrEmpty( DateDays))
            {
                DateDays = DateTime.Now.ToString("yyyyMMdd");
            }
            else
            {
                string nowdays = DateTime.Now.ToString("yyyyMMdd");
                if (DateDays != nowdays)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return false;
        }
        public static int WriteLog(string LogType, string LogText, string logfile = "")
        {
            int i = 0;
            while (ListLock||i<20)
            {
                i++;
                Thread.Sleep(2);
                if (!ListLock)
                {
                    ListLock = true;
                    break;
                }
                
            }
            string logtime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.FFF");
            LogProcessList.Add(new string[4] { LogType, LogText, logtime, logfile });
            ListLock = false;
            Console.WriteLine(logtime + " " + LogType + " " + LogText);
            return 0;
        }
        public static void InitLogFile(string logpath = "")
        {
            System.Console.WriteLine(AppDomain.CurrentDomain.BaseDirectory);
            List<string> list = new List<string>();
            if (logpath == "")
            {
                LogPath = AppDomain.CurrentDomain.BaseDirectory + "\\" + DateTime.Now.ToString("yyyyMMdd") + "\\";
            }
            else
            {
                LogPath = logpath;
            }
            
            LogFile = LogPath + DateTime.Now.ToString("yyMMdd") + ".log";
            list.Add(LogFile);
            

            DirectoryInfo di = new DirectoryInfo(LogPath);
            if (!di.Exists)
            {
                di.Create();
            }
            
            foreach (string s in list)
            {
                FileInfo fi = new FileInfo(s);
                if (fi.Exists)
                {
                    //fi.Delete();
                }
                else
                {
                    StreamWriter sw = new StreamWriter(s, true);
                    sw.Close();
                }
                

            }
            
            
        }

        public static void LogStop(int schedulerstoptime = 0)
        {
            Thread.Sleep(schedulerstoptime);
            ThreadStop = false;
        }

        public static void LoggingThread()
        {

            while (ThreadStop)
            {
                try
                {
                    if (!Logging.ListLock && Logging.LogProcessList.Count > 0)
                    {

                        Logging.ListLock = true;
                        foreach (string[] element in Logging.LogProcessList)
                        {
                            Logging.BWriteLog(element[0], element[1], element[2], element[3]);
                        }
                        Logging.LogProcessList.Clear();
                        Logging.ListLock = false;


                    }
                }
                catch (Exception ex)
                {
                    Logging.WriteLog("SystemError", "Loging Recorder :" + ex.Message);
                }
                Thread.Sleep(1000);
            }


        }


    }
}
