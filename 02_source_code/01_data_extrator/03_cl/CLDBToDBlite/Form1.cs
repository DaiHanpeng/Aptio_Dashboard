using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using System.Configuration;

namespace CLDBToDBlite
{
    public partial class Form1 : Form
    {
        string filelist = ConfigurationManager.AppSettings["filelist"];
        public Form1()
        {
            InitializeComponent();
        }

        private void B_Browser_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog dialog = new FolderBrowserDialog();
            dialog.Description = "请选择文件路径";
            if (dialog.ShowDialog() == DialogResult.OK)
            {
                string foldPath = dialog.SelectedPath;
                TB_Path.Text = foldPath;
            }
        }

        public void mainprocess()
        {
            if (Tools.CheckFileList(TB_Path.Text,filelist) < 0 )
            {
                MessageBox.Show("导出文件不全，请检查！", "提示");
                return;
            }
            Dictionary<string, DataTable> DDT;
            DDT = Tools.GetDatatable(TB_Path.Text, filelist);
            MakeNewTable.PrimaryKeyForDatatable(ref DDT);
            DirectoryInfo di = new DirectoryInfo( TB_Path.Text + "\\Export\\");
            if (!di.Exists)
            {
                di.Create();
            }
            //patient
            DataTable dt_patient =  MakeNewTable.cl_patient(DDT);
            Tools.DataTableToCSV(dt_patient, TB_Path.Text + "\\Export\\" + "cl_patient.csv");
            dt_patient.Dispose();
            //sample
            DataTable dt_sample = MakeNewTable.cl_sample(DDT);
            Tools.DataTableToCSV(dt_sample, TB_Path.Text + "\\Export\\" + "cl_sample.csv");
            dt_sample.Dispose();
            //request
            DataTable dt_requested_tests = MakeNewTable.cl_requested_tests(DDT);
            Tools.DataTableToCSV(dt_requested_tests, TB_Path.Text + "\\Export\\" + "cl_requested_tests.csv");
            dt_requested_tests.Dispose();
            //result
            DataTable dt_result = MakeNewTable.cl_result(DDT);
            Tools.DataTableToCSV(dt_result, TB_Path.Text + "\\Export\\" + "cl_result.csv");
            dt_result.Dispose();
            //flag
            DataTable dt_flag = MakeNewTable.cl_flag(DDT);
            Tools.DataTableToCSV(dt_flag, TB_Path.Text + "\\Export\\" + "cl_flag.csv");
            dt_flag.Dispose();
            //test
            DataTable dt_test = MakeNewTable.cl_test(DDT);
            Tools.DataTableToCSV(dt_test, TB_Path.Text + "\\Export\\" + "cl_tests.csv");
            dt_test.Dispose();
            DDT.Clear();
            GC.Collect();
            MessageBox.Show("已完成！", "提示");
        }

        private void button2_Click(object sender, EventArgs e)
        {
            mainprocess();
        }
    }
}
