using System;
using System.IO;
using System.Configuration;
using System.Threading;

namespace WriteCSVToDB_Core
{
    class Program
    {
        public static string logpath;
        public static string projectid;
        public static string DataBaseName;
        public static Boolean CanUpdateStatus = false;
        static void Main(string[] args)
        {
            
            string id, dbname;
            try
            {
                Console.WriteLine(args.Length.ToString());
                if (args.Length < 3)
                {
                    id = "";
                    dbname = "";
                    logpath = AppDomain.CurrentDomain.BaseDirectory;
                    GetPara(ref id,ref dbname);
                    projectid = id;
                    DataBaseName = dbname;

                }
                else
                {
                    logpath = args[0];
                    projectid = args[1];
                    DataBaseName = args[2];
                }
                MakeThread();
                

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


            //Console.ReadLine();
            Console.WriteLine("Hello World!");
        }

        static void MainThread()
        {
            try
            {

                //check status db
                if (!StatusRefresh.CheckStatusDBConnect(projectid))
                {
                    Console.WriteLine("Can not connect datacake db or project ID is not exists! The process status will not update!");
                }
                else
                {
                    CanUpdateStatus = true;
                }

                //init target db;

                MysqlClass.InitDatabasePara(DataBaseName);

                if (MysqlClass.ConnectDb() == null)
                {
                    Console.WriteLine("Can not connect target database,please check setting! press any key to exit!");
                    Logging.LogStop();
                    return;
                }

                //check logfile

                string logfoldname = ConfigurationManager.AppSettings["LogFoldName"];
                string folderpath = logpath + "\\" + logfoldname;
                DirectoryInfo di = new DirectoryInfo(folderpath);
                if (!di.Exists)
                {
                    Console.WriteLine("Wrong Path, Try To Use Application Current Path!");
                    DirectoryInfo cdi = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory + "LogFoldName");
                    if (!cdi.Exists)
                    {
                        Console.WriteLine("Wrong Path, application will close!");
                        Logging.LogStop();
                        return;
                    }
                }
                CSVToDB.Process(folderpath, projectid, DataBaseName);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Logging.LogStop();
            }
            Logging.LogStop();

        }

        static void MakeThread()
        {
            Logging.InitLogFile();
            Thread TLogThread = new Thread(Logging.LoggingThread);
            TLogThread.Name = "LogThread";
            TLogThread.Start();
            Thread.Sleep(10);

            Thread TMainThread = new Thread(MainThread);
            TMainThread.Name = "MainThread";
            TMainThread.Start();
            
            
            //Console.ReadLine();
            //Console.ReadLine();
            //Console.ReadLine();

        }




        static void GetPara(ref string StatusRecoerdID,ref string UpdateDBName)
        {
            string id, dbname;
            Console.WriteLine("Argument less than 3, Will use current folder as log root path,and should be input status refresh id and target dbname!");
            Console.Write("Please input Status Record ID(if do not need update the process status, just press enter key):");
            id = Console.ReadLine();
            id = id.Trim();
            Console.Write("Please input the target Database Name,if use default ,just enter:");
            dbname = Console.ReadLine();
            if (dbname.Trim() == "")
            {
                dbname = ConfigurationManager.AppSettings["DBDefaultName"];
            }
            //Console.WriteLine(id);
            //Console.WriteLine(dbname);
            StatusRecoerdID = id;
            UpdateDBName = dbname;
        }
        
    }
}
