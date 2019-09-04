using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Configuration;

namespace CLDBToDBlite
{
    public static class MakeNewTable
    {
        public static DataTable cl_patient(Dictionary<string,DataTable> Ddt)
        {
            DataTable dt_patient = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_patient"];
            MakeDataTableColumns(column,ref dt_patient);
            
            foreach (DataRow row in Ddt["Patient"].Rows)
            {
                DataRow dr = dt_patient.NewRow();
                dr["pid"] = row["Identifier"];
                dr["sex"] = row["Sex"];
                dr["birthday"] = row["BirthDate"];
                dr["location"] = "?";
                dt_patient.Rows.Add(dr);
            }
            Ddt["Patient"].Clear();
            GC.Collect();

            return dt_patient;
        }

        public static DataTable cl_sample(Dictionary<string, DataTable> Ddt)
        {
            DataTable dt_sample = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_sample"];
            MakeDataTableColumns(column, ref dt_sample);

            foreach (DataRow row in Ddt["Sample"].Rows)
            {
                DataRow dr = dt_sample.NewRow();
                dr["sid"] = row["Identifier"];
                dr["tube_type"] = row["ContainerType_Name"];
                dr["sample_type"] = row["Species_Name"];
                dr["pid"] = row["Patient_Identifier"];
                dr["priority"] = row["Priority"];
                dr["create_datetime"] = row["CollectionTime"];
                dt_sample.Rows.Add(dr);
            }
            

            return dt_sample;
        }

        public static DataTable cl_requested_tests(Dictionary<string, DataTable> Ddt)
        {
            DataTable dt_requested_tests = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_requested_tests"];
            MakeDataTableColumns(column, ref dt_requested_tests);

            foreach (DataRow row in Ddt["Request"].Rows)
            {
                DataRow dr = dt_requested_tests.NewRow();
                DataRow[] rows = Ddt["Sample"].Select("Identifier = '" + row["Sample_Identifier"] + "'");
                if (rows.Length > 0)
                {
                    dr["pid"] = rows[0]["Patient_Identifier"];
                }
                else
                {
                    dr["pid"] = "?";
                }
                dr["sid"] = row["Sample_Identifier"];
                dr["test_name"] = row["Test_Name"];
                dr["create_datetime"] = row["PatientHistoryTime"];
                dr["action_code"] = "N";
                if (row["Status"].ToString() == "Uploaded")
                {
                    dr["complete_status"] = "1";
                }
                else
                {
                    dr["complete_status"] = "0";
                }
                dt_requested_tests.Rows.Add(dr);
                
            }
            Ddt["Sample"].Clear();
            GC.Collect();

            return dt_requested_tests;
        }

        public static DataTable cl_result(Dictionary<string, DataTable> Ddt)
        {
            DataTable dt_result = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_result"];
            MakeDataTableColumns(column, ref dt_result);

            foreach (DataRow row in Ddt["Result"].Rows)
            {
                DataRow dr = dt_result.NewRow();
                dr["id"] = row["Id"];
                dr["sid"] = row["Request_Sample_Identifier"];
                //    ["Sample"].Select("Identifier = '" + row["Sample_Identifier"] + "'")[0]["Patient_Identifier"];
                dr["instrument_id"] = row["Method_Instrument_Name"];
                dr["test_name"] = row["Request_Test_Name"];
                dr["time_stamp"] = row["TestCompletedTime"]; 
                dr["dilution_profile"] = row["AutoDilutionCondition"];
                dr["dilution_factor"] = row["AutoDilutionCoeff"];
                

                dr["result"] = row["Value"];


                string test = dr["test_name"].ToString();
                string instr = dr["instrument_id"].ToString();
                DataRow[] datarows = Ddt["Method"].Select("Test_Name = '" + test + "' and Instrument_Name = '" + instr + "'");
                if (datarows.Length > 0)
                {
                    dr["aspect"] = datarows[0]["PatientResultSelector_Name"];
                }
                else
                {
                    dr["aspect"] = "?";
                }


                string resultid = row["Id"].ToString();
                DataRow[] flagrows = Ddt["ResultFlag"].Select("Result = '" + resultid + "'");
                if (flagrows.Length > 0)
                {
                    dr["flagged"] = "1";
                }
                else
                {
                    dr["flagged"] = "0";
                }
                string sid = dr["sid"].ToString();
                string testname = dr["test_name"].ToString();
                DataRow[] requestrow = Ddt["Request"].Select("Sample_Identifier = '" + sid + "' and Test_Name = '" + testname + "'");
                if (requestrow.Length > 0)
                {
                    dr["NS"] = requestrow[0]["NormSeverity"];
                    dr["DS"] = requestrow[0]["DeltaNormSeverity"];
                    dr["IS"] = requestrow[0]["InstrumentSeverity"];
                }
                else
                {
                    dr["NS"] = "0";
                    dr["DS"] = "0";
                    dr["IS"] = "0";
                }
                
                dr["QS"] = row["QCSeverity"];
                dr["CS"] = "?";
                dr["SS"] = "?";
                dr["OS"] = "?";
                dr["LS"] = "?";
                dr["auto_val_result"] = "?";
                dr["tat_inlabbing_result"] = "?";

                
                dt_result.Rows.Add(dr);
                
            }
            Ddt["Request"].Clear();
            Ddt["Method"].Clear();
            Ddt["Result"].Clear();
            GC.Collect();

            return dt_result;
        }

        public static DataTable cl_flag(Dictionary<string, DataTable> Ddt)
        {
            DataTable dt_flag = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_flag"];
            MakeDataTableColumns(column, ref dt_flag);

            foreach (DataRow row in Ddt["ResultFlag"].Rows)
            {
                DataRow dr = dt_flag.NewRow();
                dr["code"] = row["InstrumentFlag_Code"];
                dr["rid"] = row["Result"];
                
                dt_flag.Rows.Add(dr);
            }

            Ddt["ResultFlag"].Clear();
            GC.Collect();
            return dt_flag;
        }

        public static DataTable cl_test(Dictionary<string, DataTable> Ddt)
        {
            DataTable dt_test = new DataTable();
            string column = ConfigurationManager.AppSettings["cl_tests"];
            MakeDataTableColumns(column, ref dt_test);
            

            foreach (DataRow row in Ddt["LASChannel"].Rows)
            {
                if (row["CodingSystem_Name"].ToString() != "?")
                {
                    string codingsystem = row["CodingSystem_Name"].ToString();
                    DataRow[] rows = Ddt["TestCode"].Select("CodingSystem_Name = '" + codingsystem + "'");
                    if (rows.Length >0)
                    {
                        foreach (DataRow r in rows)
                        {
                            DataRow dr = dt_test.NewRow();
                            dr["test_name"] = r["Test_Name"];
                            dr["test_code_las"] = r["OutboundValue"];
                            dt_test.Rows.Add(dr);
                        }
                    }

                }
                
            }
            Ddt["LASChannel"].Clear();
            Ddt["TestCode"].Clear();
            GC.Collect();

            return dt_test;
        }



        public static void MakeDataTableColumns(string columnslist,ref DataTable dt)
        {
            string[] columns = columnslist.Split(',');
            foreach(string columnname in columns)
            {
                DataColumn dc = new DataColumn(columnname.Trim());
                dt.Columns.Add(dc);
            }
        }

        public static void PrimaryKeyForDatatable(ref Dictionary<string, DataTable> Ddt)
        {
            DataTable temp = Ddt["Sample"];
            string pk = "Identifier";
            SetPrimaryKey(ref temp, pk);
            Ddt["Sample"] = temp;

            temp = Ddt["Patient"];
            pk = "Identifier";
            SetPrimaryKey(ref temp, pk);
            Ddt["Patient"] = temp;

            //temp = Ddt["Result"];
            //pk = "Id";
            //SetPrimaryKey(ref temp, pk);
            //Ddt["Result"] = temp;

            temp = Ddt["Request"];
            pk = "Sample_Identifier,Test_Name";
            SetPrimaryKey(ref temp, pk);
            Ddt["Request"] = temp;

        }
        public static void SetPrimaryKey(ref DataTable dt,string PrimaryKeyL)
        {
            List<DataColumn> clist = new List<DataColumn>();
            List<string> Plist = new List<string>();
            DataColumn[] pcolumn;
            string[] PrimaryKey = PrimaryKeyL.Split(',');
            if (PrimaryKey.Length == 0)
            {

                Plist.Add(PrimaryKeyL);
                PrimaryKey = Plist.ToArray();
            }
            
            foreach (string col in PrimaryKey)
            {
                clist.Add(dt.Columns[col]);
            }
            pcolumn = clist.ToArray();
            dt.PrimaryKey = pcolumn;
        }
    }
}
