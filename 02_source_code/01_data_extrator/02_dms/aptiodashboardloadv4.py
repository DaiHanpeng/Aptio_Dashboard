#!/usr/bin/python
import MySQLdb
import _mysql_exceptions
import sys
import zipfile
import os
import re
import ConfigParser
from datetime import datetime
import logging

# in v4:
# update datacake folder instead of production database about file uploading process
# manage path argument by processing only the latest zip file found inside the specified path
# manage log file


# function to insert dms tables from backup sql file to temporary dms db
 
def insertdmstable(dmstable,dmssqlfilepath,mycursor):
    statement = ""
    linenum=0
    line_start=0
    line_end=0
    flag_delimiter=0
    for line in open(dmssqlfilepath):
        linenum=linenum+1
        if re.search('-- Table structure for table `'+dmstable+'`', line):
            logging.warning("Process table starts on line number "+ str(linenum))
            #print("Process table starts on line number ",linenum)
            line_start=linenum
        if re.search('-- Table structure for table', line) and line_start!=0 and line_end==0 and linenum>line_start:
            logging.warning("Process table ends on line number " + str(linenum))
            #print("Process table ends on line number ",linenum)
            line_end=linenum
    
        if  line_start!=0 and line_end==0:
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if re.match(r'DELIMITER ;', line):  # ignore sql DELIMITER statement and enable the flag_delimiter
                flag_delimiter=0                
                if re.match(r'DELIMITER ;;', line):  # ignore sql DELIMITER statement and enable the flag_delimiter
                    flag_delimiter=1
                continue
            if flag_delimiter==1:
                if not re.search(r";;", line):  # keep appending lines that don't end in ';'
                    statement = statement + line
                else:  # when you get a line ending in ';' then exec statement and reset for next statement
                    statement = statement + line
                    try:
                        mycursor.execute(statement)
                        #print(statement)
                    except Exception as e:
                        logging.warning(e)
                        #print (e)
                        logging.warning("affected line of dms sql file is: ", linenum)
                        #print("affected line of dms sql file is: ", linenum)
                        logging.warning(statement)
                        #print(statement)
                        logging.warning(flag_delimiter)
                        #print(flag_delimiter)
                        exit()
                    statement = ""                
            else:
                if not re.search(r";", line):  # keep appending lines that don't end in ';'
                    statement = statement + line
                else:  # when you get a line ending in ';' then exec statement and reset for next statement
                    statement = statement + line
                    try:
                        mycursor.execute(statement)
                        #print(statement)
                    except Exception as e:
                        logging.warning(e)
                        #print (e)
                        logging.warning("affected line of dms sql file is: ", linenum)
                        #print("affected line of dms sql file is: ", linenum)
                        logging.warning(statement)
                        #print(statement)
                        logging.warning(flag_delimiter)
                        #print(flag_delimiter)
                        exit()
                    statement = ""
    logging.warning('Table '+dmstable+' processed')
    #print('Table '+dmstable+' processed')

#function to update the datasource table in the production database
def updatedatasourcetable(dbhandler,productiondb,datasourcetable,datasourcetableid,mycursor,todmsstatus,todmsprocess,todmsremark):
    mycursor.execute("""update `"""+productiondb+"""`.`"""+datasourcetable+"""` set ToDMSStatus="""+str(todmsstatus)+""", ToDMSProcess='"""+str(todmsprocess)+"""', ToDMSRemark ='"""+str(todmsremark)+"""' where id="""+str(datasourcetableid)+""";""")
    dbhandler.commit()
    
def main():

    ############## READ CONFIGURATION INI FILE #############
    Config = ConfigParser.ConfigParser()
    Config.read("dashconf.ini")
    options = Config.options('Default')
    dict1={}
    for option in options:
        temp = option
        globals()[temp] = str(Config.get('Default', option))

    ############## CHECK SCRIPT ARGUMENTS ###############
    if (len(sys.argv) != 4):
        print("Arguments should be 3: 1*the full path of the file to process; 2*id of the datacake.sync_datasource table ID; 3*id of the hospital DB --> Exit")
        sys.exit(0)
    
    print("This is the path to the file to process: ", sys.argv[1])
    print("This is the datacake.sync_datasource table ID:", sys.argv[2])
    print("This is the id of the hospital: " , sys.argv[3])
    
    zipfilepath=sys.argv[1]
    if not os.path.exists(zipfilepath):
        print('The backup up in arguments doesn\'t exists! Exit.. ')
        sys.exit(0)
    syncdatasourcetableid=sys.argv[2]
    realdashboarddb=sys.argv[3]

    ############## OPEN DMS BACKUP FILE AND LOG FILE ##########

    files = []
    mindate=datetime(0001,01,01)
    minfile=""
    # r=root, d=directories, f = files
    for r, d, f in os.walk(zipfilepath):
        for file in f:
            if '.zip' in file:
                files=file.split("_")
                curdate=datetime.strptime(files[2],'%Y%m%d')
                if curdate > mindate:
                    mindate=curdate
                    minfile=file
                    
    logging.basicConfig(filename=minfile+'.log', filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.warning("Processing " + minfile)
    
    with zipfile.ZipFile(zipfilepath+minfile) as zf:
        zf.extractall(pwd=dmsbackuppw)
    
    logging.warning(minfile + " extracted")
    
    ############## OPEN MYSQL CONNECTION ##############
    db = MySQLdb.connect(host=mysqlhostip,  # your host 
                     user=mysqluser,       # username
                     passwd=mysqlpw)     # password
    cur = db.cursor()
    
    ############## CHECK LAST UPLOAD STATUS AND REMOVE LAST INSERTED DATA IN CASE OF ERROR ##############
    cur.execute("""select ToDMSStatus from `"""+syncdatasourcedb+"""`.`"""+syncdatasourcetable+"""` where id="""+str(syncdatasourcetableid)+""";""")
    ToDMSStatus=cur.fetchone()
    if ToDMSStatus!=None:
        ToDMSStatus=str(ToDMSStatus[0])
        #print("Debug:" + ToDMSStatus)
        if ToDMSStatus=='1':
                cur.execute("""select ToDMSProcess from `"""+syncdatasourcedb+"""`.`"""+syncdatasourcetable+"""` where id="""+str(syncdatasourcetableid)+""";""")
                ToDMSProcess=cur.fetchone()
                if ToDMSProcess!=None:
                    ToDMSProcess=str(ToDMSProcess[0])
                else:
                    ToDMSProcess='0-0'
                updatedatasourcetable(db,syncdatasourcedb,syncdatasourcetable,syncdatasourcetableid,cur,0,str(int(ToDMSProcess.split('-')[0])-1)+"-"+str(int(ToDMSProcess.split('-')[0])-1),'')
                logging.warning("SYNC_DATASOURCE was interrupted with error on the last run. Rolled back to previous consistent status.")
                #print("SYNC_DATASOURCE was interrupted with error on the last run. Rolled back to previous consistent status.")
    
    flagconsistent=None
    #select uploaderror from `"""+realdashboarddb+"""`.`dms_upload_log` where id=(select max(id) from """+realdashboarddb+""".`dms_upload_log`);
    cur.execute("""select `uploaderror` from `"""+realdashboarddb+"""`.`dms_upload_log` where id=(select max(id) from """+realdashboarddb+""".`dms_upload_log`);""")
    flagconsistent=cur.fetchone()
    
    if flagconsistent!=None:
        flagconsistent=int(flagconsistent[0])
    else:
        flagconsistent=0
    if flagconsistent==0:
        logging.warning("Last updload completed succesfully. Now continuing with new data.")
        #print("Last updload completed succesfully. Now continuing with new data.")
    else:
        logging.warning("The last upload ended with error. Rolling back to the last consistent status...")
        #print("The last upload ended with error. Rolling back to the last consistent status...")
        
        cur.execute("""select sum(npatient) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnpatient=cur.fetchone()
        if lastnpatient!=None:
            lastnpatient=int(lastnpatient[0])            
        else:
            lastnpatient=0
            logging.warning("0 patient record to cancel")
            #print("0 patient record to cancel")

        cur.execute("""select sum(nsample) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnsample=cur.fetchone()
        if lastnsample!=None:
            lastnsample=int(lastnsample[0])            
        else:
            lastnsample=0
            logging.warning("0 sample record to cancel")
            #print("0 sample record to cancel")        
        
        cur.execute("""select sum(nreqtest) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnreqtest=cur.fetchone()
        if lastnreqtest!=None:
            lastnreqtest=int(lastnreqtest[0])            
        else:
            lastnreqtest=0
            logging.warning("0 requested test record to cancel")
            #print("0 requested test record to cancel")                
        
        cur.execute("""select sum(nresult) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnresult=cur.fetchone()
        if lastnresult!=None:
            lastnresult=int(lastnresult[0])            
        else:
            lastnresult=0
            logging.warning("0 result record to cancel")
            #print("0 result record to cancel")        
        
        cur.execute("""select sum(nflag) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnflag=cur.fetchone()
        if lastnflag!=None:
            lastnflag=int(lastnflag[0])            
        else:
            lastnflag=0
            logging.warning("0 flag record to cancel")
            #print("0 flag record to cancel")        
        
        cur.execute("""select sum(ntests) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastntests=cur.fetchone()
        if lastntests!=None:
            lastntests=int(lastntests[0])            
        else:
            lastntests=0
            logging.warning("0 tests record to cancel")
            #print("0 tests record to cancel")        
               
        logging.warning("deleting new data...")
        #print("deleting new data...")
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_patient` where `id`>"""+str(lastnpatient)+""";""")
        db.commit()
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_sample` where `id`>"""+str(lastnsample)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_requested_tests` where `id`>"""+str(lastnreqtest)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_result` where `id`>"""+str(lastnresult)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_flag` where `id`>"""+str(lastnflag)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_tests` where `id`>"""+str(lastntests)+""";""")
        db.commit() 
        logging.warning("last consistent status reloaded")
        #print("last consistent status reloaded")

    
    ############### UPDATE SYNC_DATASOURCE TABLE ##################
    cur.execute("""select ToDMSProcess from `"""+syncdatasourcedb+"""`.`"""+syncdatasourcetable+"""` where id="""+str(syncdatasourcetableid)+""";""")
    ToDMSProcess=cur.fetchone()
    if ToDMSProcess!=None and ToDMSProcess!=' ':
        ToDMSProcess=str(ToDMSProcess[0])
        #print("Debug1:" + ToDMSProcess)
    else:
        #print("Debug2:" + ToDMSProcess)
        ToDMSProcess='0-0'
    updatedatasourcetable(db,syncdatasourcedb,syncdatasourcetable,syncdatasourcetableid,cur,1,str(int(ToDMSProcess.split('-')[0])+1)+"-"+str(int(ToDMSProcess.split('-')[0])+1),'processing...')
    logging.warning("SYNC_DATASOURCE updated")
    #print("SYNC_DATASOURCE updated")
    
    ############## CREATE temporary dashboard DB ###############
    try:
        logging.warning("Creating temporary dashboard database...")
        #print("Creating temporary dashboard database...")
        cur.execute("create database dashboardtemp")
        logging.warning("temporary dashboard database created")
        #print("temporary dashboard database created")
        cur.execute("use dashboardtemp")
        statement = ""
        linenum=0
        for line in open(dashboarddb):
            linenum=linenum+1
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if not re.search(r";", line):  # keep appending lines that don't end in ';'
                statement = statement + line
            else:  # when you get a line ending in ';' then exec statement and reset for next statement
                statement = statement + line
                try:
                    cur.execute(statement)
                except Exception as e:
                    logging.warning(e)
                    #print (e)
                    logging.warning("affected line of dashboard sql file is: ", linenum)
                    #print("affected line of dashboard sql file is: ", linenum)
                    logging.warning(statement)
                    #print(statement)
                statement = ""    
        logging.warning("temporary dashboard database filled")
        #print("temporary dashboard database filled")
    except _mysql_exceptions.ProgrammingError as prog_error:
        logging.warning(prog_error[1])
        #print(prog_error[1])
        logging.warning('deleting database...')
        #print('deleting database...')
        cur.execute("drop database dashboardtemp")
        logging.warning('Deletion complete')
        #print('Deletion complete')
        cur.execute("create database dashboardtemp")
        logging.warning("temporary dashboard database created")
        #print("temporary dashboard database created")
        cur.execute("use dashboardtemp")
        statement = ""
        linenum=0
        for line in open(dashboarddb):
            linenum=linenum+1
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if not re.search(r";", line):  # keep appending lines that don't end in ';'
                statement = statement + line
            else:  # when you get a line ending in ';' then exec statement and reset for next statement
                statement = statement + line
                try:
                    cur.execute(statement)
                except Exception as e:
                    logging.warning(e)
                    #print (e)
                    logging.warning("affected line of dashboard sql file is: ", linenum)
                    #print("affected line of dashboard sql file is: ", linenum)
                    logging.warning(statement)
                    #print(statement)
                statement = ""    
        logging.warning("temporary dashboard database filled")
        #print("temporary dashboard database filled")
    
    ############## CREATE temporary dms DB ###############    
    
    # run SET commands at the beginning of dms sql backup file
    try:
        logging.warning("Creating temporary dms database...")
        #print("Creating temporary dms database...")
        cur.execute("create database dmstemp")
        logging.warning("temporary dms database created")
        #print("temporary dms database created")
        cur.execute("use dmstemp")        
    except _mysql_exceptions.ProgrammingError as prog_error:
        logging.warning(prog_error[1])
        #print(prog_error[1])
        logging.warning('deleting database...')
        #print('deleting database...')
        cur.execute("drop database dmstemp")
        logging.warning('Deletion complete')
        #print('Deletion complete')
        cur.execute("create database dmstemp")
        logging.warning("temporary dms database created")
        #print("temporary dms database created")
        cur.execute("use dmstemp")
    file=os.listdir(unzipfolder)
    logging.warning("processing: " + str(file[0]))
    #print("processing: ", file[0])
    linenum=0
    for line in open(unzipfolder+file[0]):
        linenum=linenum+1
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if re.search(r'DROP TABLE IF EXISTS',line):    
            break     
        if re.search(r';', line):  # run SET commands at the beginning of dms sql backup file
            try:
                cur.execute(line)
            except Exception as e:
                logging.warning("\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args)))
                #print "\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args))
   
    # insert anagpatient, anagpatient_details, reqtest, orders, reqtestresult, testinstrument and reqtube into the dms temporary database;
    dmstables=['anagpatient', 'anagpatient_details', 'reqtest', 'orders', 'reqtestresult', 'testinstrument', 'reqtube','eventautomation','instrument','testlab']
    
    for i in dmstables:
        insertdmstable(i,unzipfolder+file[0],cur)
    
    # number of entries in the dms temp database
    cur.execute("""select count(*) from `dmstemp`.`anagpatient`;""")
    numpatientdmsbackup=int(cur.fetchone()[0])
    logging.warning("number of patient records in dms backup file: " + str(numpatientdmsbackup))
    #print("number of patient records in dms backup file: " + str(numpatientdmsbackup))
    cur.execute("""select count(*) from `dmstemp`.`reqtube`;""")
    numsampledmsbackup=int(cur.fetchone()[0])
    logging.warning("number of sample records in dms backup file: " + str(numsampledmsbackup))
    #print("number of sample records in dms backup file: " + str(numsampledmsbackup))
    #select count(*) from reqtest as a inner join testlab as b on a.codtest=b.codtest where b.codresult="A" and b.codparameter=1;
    cur.execute("""select count(*) from `dmstemp`.`reqtest` as a inner join `dmstemp`.`testlab` as b on a.`codtest`=b.`codtest` where b.`codresult`="A" and b.`codparameter`=1;""")
    numreqtestdmsbackup=int(cur.fetchone()[0])
    logging.warning("number of requested test records in dms backup file: " + str(numreqtestdmsbackup))
    #print("number of requested test records in dms backup file: " + str(numreqtestdmsbackup))    
    cur.execute("""select count(*) from `dmstemp`.`reqtestresult` as a inner join `dmstemp`.`testlab` as b on a.`codtest`=b.`codtest` where b.`codresult`="A" and b.`codparameter`=1;""")
    numresultdmsbackup=int(cur.fetchone()[0])
    logging.warning("number of result records in dms backup file: " + str(numresultdmsbackup))
    #print("number of result records in dms backup file: " + str(numresultdmsbackup))    
    #select count(*) from reqtestresult where jsnflaginstrument!="[]" and jsnflaginstrument!="";
    cur.execute("""select count(*) from `dmstemp`.`reqtestresult` where `jsnflaginstrument`!="[]" and `jsnflaginstrument`!="";""")
    numflagdmsbackup=int(cur.fetchone()[0])
    logging.warning("number of dms flag records in dms backup file: " + str(numflagdmsbackup))
    #print("number of dms flag records in dms backup file: " + str(numflagdmsbackup))    
    #select count(*) from testinstrument where codiid='INPECO'
    cur.execute("""select count(*) from `dmstemp`.`testinstrument` where `codiid`='INPECO';""")
    numtestsdmsbackup=int(cur.fetchone()[0])
    logging.warning("number of tests records in dms backup file: " + str(numtestsdmsbackup))
    #print("number of tests records in dms backup file: " + str(numtestsdmsbackup))    
    
    ############### INSERT data in temporary dashboard DB ##################
    logging.warning("transmitting data from temporary DMS database to temporary Dashboard database... ")
    #print("transmitting data from temporary DMS database to temporary Dashboard database... ")
    
    #dms_patient
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_patient` (`pid`,`birthday`,`sex`,`location`) select `codpid`,`datbirth`,`flgsex`,`codlocation` from `dmstemp`.`anagpatient` as a inner join `dmstemp`.`anagpatient_details` as b on a.`codaid`=b.`codaid`""")
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print(e)
    
    #dms_sample
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_sample` (`sid`,`tube_type`,`sample_type`,`pid`,`priority`,`create_datetime`) select a.`codsid`,a.`valdiameter`,a.`codtypesample`,d.`id`,a.`flgstat`,b.`datorder` from `dmstemp`.`reqtube` as a inner join `dmstemp`.`orders` as b on a.`codoid`=b.`codoid` inner join `dmstemp`.`anagpatient` as c on b.`codaid`=c.`codaid` inner join `dashboardtemp`.`dms_patient` as d on c.`codpid`=d.`pid`""")
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print()
        
    #dms_requested_test
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_requested_tests` (`sid`,`test_name`,`create_datetime`,`action_code`) select a.`codsid`,a.`codtest`,b.`datorder`,a.`codactioncode` from `dmstemp`.`reqtest` as a inner join `dmstemp`.`orders` as b on a.`codoid`=b.`codoid` inner join `dmstemp`.`testlab` as c on a.`codtest`=c.`codtest` where c.`codresult`="A" and c.`codparameter`=1; """)
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print()
     
    #dms_result
    try:
        cur.execute("""alter table dashboardtemp.dms_result add flagvalue text;""")
        cur.execute("""insert ignore into `dashboardtemp`.`dms_result` (`sid`,`instrument_id`,`test_name`,`time_stamp`,`dilution_factor`,`result`,`aspect`,`flagged`,`flagvalue`,`auto_val_result`,`tat_inlabbing_result`)  select a.`codsid`,a.`codinstrument`,a.`codtest`,a.`datresult`,a.`valdilution`,a.`valresult1`,b.`validentifier1`,IF(a.`jsnflaginstrument`!="[]",1,0),a.`jsnflaginstrument`,a.`flgvalid`,timediff(a.`datresult`,c.`datcheckin`) from `dmstemp`.`reqtestresult` as a left join `dmstemp`.`testinstrument` as b on a.`codtest`=b.`codtest` left join (select `codsid`,`codinstrument`,min(`datevent`) as `datcheckin` from `dmstemp`.`eventautomation` where `txtevent`='L001' group by `codsid`) as c on a.`codsid`=c.`codsid` inner join `dmstemp`.`instrument` as d on b.`codiid`=d.`codiid` and a.`codinstrument`=d.`codinstrument` where b.`codiid`!='HOSTASTMmCH' and b.`codiid`!='INPECO' and b.`codparameter`=1;""")
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print()
    
    #dms_flag
    try:
        cur.execute("""ALTER TABLE `dashboardtemp`.`dms_flag` MODIFY `code` text;""")
        cur.execute("""insert ignore into `dashboardtemp`.`dms_flag` (`code`,`rid`) select `flagvalue`,`id` from `dashboardtemp`.`dms_result` where `flagvalue`!="[]" and `flagvalue`!="";""")
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print()

    #dms_tests
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_tests` (`test_name`,`test_code_las`) select `codtest`,`codtestsend` from `dmstemp`.`testinstrument` where `codiid`='INPECO';""")
        db.commit()
    except Exception as e:
        logging.warning(e)
        #print()
    
    ############## COLLECT REAL DASHBOARD DB CURRENT STATUS AND NUMBER OF NEW DATA TO INSERT################
    # Current number of entries in the production database
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    numcurpatient=int(cur.fetchone()[0])
    logging.warning("Current number of dms patient records in production dashboard database: " + str(numcurpatient))
    #print("Current number of dms patient records in production dashboard database: " + str(numcurpatient))
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    numcursample=int(cur.fetchone()[0])
    logging.warning("Current number of dms sample records in production dashboard database: " + str(numcursample))
    #print("Current number of dms sample records in production dashboard database: " + str(numcursample))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    numcurreqtest=int(cur.fetchone()[0])
    logging.warning("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest))
    #print("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_result`;""")
    numcurresult=int(cur.fetchone()[0])
    logging.warning("Current number of dms result records in production dashboard database: " + str(numcurresult))
    #print("Current number of dms result records in production dashboard database: " + str(numcurresult))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    numcurflag=int(cur.fetchone()[0])
    logging.warning("Current number of dms flag records in production dashboard database: " + str(numcurflag))
    #print("Current number of dms flag records in production dashboard database: " + str(numcurflag))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    numcurtests=int(cur.fetchone()[0])
    logging.warning("Current number of dms tests records in production dashboard database: " + str(numcurtests))
    #print("Current number of dms tests records in production dashboard database: " + str(numcurtests))    

    # Number of new entries to insert
    cur.execute("""select count(*) from `dashboardtemp`.`dms_patient`;""")
    numnewpatient=int(cur.fetchone()[0])
    logging.warning("number of dms patient records to insert in production dashboard database: " + str(numnewpatient))
    #print("number of dms patient records to insert in production dashboard database: " + str(numnewpatient))
    cur.execute("""select count(*) from `dashboardtemp`.`dms_sample`;""")
    numnewsample=int(cur.fetchone()[0])
    logging.warning("number of dms sample records to insert in production dashboard database: " + str(numnewsample))
    #print("number of dms sample records to insert in production dashboard database: " + str(numnewsample))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_requested_tests`;""")
    numnewreqtest=int(cur.fetchone()[0])
    logging.warning("number of dms requested test records to insert in production dashboard database: " + str(numnewreqtest))
    #print("number of dms requested test records to insert in production dashboard database: " + str(numnewreqtest))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_result`;""")
    numnewresult=int(cur.fetchone()[0])
    logging.warning("number of dms result records to insert in production dashboard database: " + str(numnewresult))
    #print("number of dms result records to insert in production dashboard database: " + str(numnewresult))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_flag`;""")
    numnewflag=int(cur.fetchone()[0])
    logging.warning("number of dms flag records to insert in production dashboard database: " + str(numnewflag))
    #print("number of dms flag records to insert in production dashboard database: " + str(numnewflag))
    cur.execute("""select count(*) from `dashboardtemp`.`dms_tests`;""")
    numnewtests=int(cur.fetchone()[0])
    logging.warning("number of dms tests records to insert in production dashboard database: " + str(numnewtests))
    #print("number of dms tests records to insert in production dashboard database: " + str(numnewtests))       
    
    ############## COMPARE RECORDS WITHIN DMS BACKUP WITH RECORDS WITHIN DASHBOARD TEMP --> IF NOT THE SAME SOME THERE IS SOME CONFIGURATION  PROBLEM IN THE SITE's DMS. ASK TO USER HOW TO PROCEED ##################
    errorflag=0
    if numpatientdmsbackup==numnewpatient:
        logging.warning("patient records to be inserted are consistent with the backup")
        #print("patient records to be inserted are consistent with the backup")
    else:
        logging.warning("patient records to be inserted are NOT consistent with the backup")
        #print("patient records to be inserted are NOT consistent with the backup")
        errorflag=1   
    if numsampledmsbackup==numnewsample:
        logging.warning("sample records to be inserted are consistent with the backup")
        #print("sample records to be inserted are consistent with the backup")
    else:
        logging.warning("sample records to be inserted are NOT consistent with the backup")
        #print("sample records to be inserted are NOT consistent with the backup")
        errorflag=1
    if numreqtestdmsbackup==numnewreqtest:
        logging.warning("requested test records to be inserted are consistent with the backup")
        #print("requested test records to be inserted are consistent with the backup")
    else:
        logging.warning("requested test records to be inserted are NOT consistent with the backup")
        #print("requested test records to be inserted are NOT consistent with the backup")
        errorflag=1
    if numresultdmsbackup==numnewresult:
        logging.warning("result records to be inserted are consistent with the backup")
        #print("result records to be inserted are consistent with the backup")
    else:
        logging.warning("result records to be inserted are NOT consistent with the backup")
        #print("result records to be inserted are NOT consistent with the backup")    
        errorflag=1 
    if numflagdmsbackup==numnewflag:
        logging.warning("flag records to be inserted are consistent with the backup")
        #print("flag records to be inserted are consistent with the backup")
    else:
        logging.warning("flag records to be inserted are NOT consistent with the backup")
        #print("flag records to be inserted are NOT consistent with the backup")
        errorflag=1
    if numtestsdmsbackup==numnewtests:
        logging.warning("test records to be inserted are consistent with the backup")
        #print("test records to be inserted are consistent with the backup")
    else:
        logging.warning("test records to be inserted are NOT consistent with the backup")
        #print("test records to be inserted are NOT consistent with the backup")
        errorflag=1
    if errorflag==1:
        logging.warning("According to my analisys there is some mismatch between the data in the dms backup and the data that I am going to insert. This is most likely due to a DMS configuration error.")
        #print("According to my analisys there is some mismatch between the data in the dms backup and the data that I am going to insert. This is most likely due to a DMS configuration error. If you think that the wrong records are few enough to not impact the statistics you can continue. Do you want to continue? [Y/N] : ")
        #while True:
        #    answer = raw_input()
        #    if answer.strip() == 'Y' or 'N':
        #        break        
        #if answer == 'N':
        #    os.remove(unzipfolder+file[0])
        #    logging.warning()
        #    print('unzipped file removed') 
        #    cur.execute("drop database dashboardtemp")
        #    cur.execute("drop database dmstemp")
        #    logging.warning()
        #    print('Temporary database removed')        
        #    db.close()
        #    sys.exit(0)
           
    ############## INSERT NEW LINE IN DMS_UPLOAD_LOG ############
    logging.warning("Inserting new log line in production database...")
    #print("Inserting new log line in production database...")
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_upload_log` (`filename`,`npatient`,`nsample`,`nreqtest`,`nresult`,`nflag`,`ntests`,`uploaderror`) values ('"""+str(zipfilepath+minfile)+"""',"""+str(numnewpatient)+""","""+str(numnewsample)+""","""+str(numnewreqtest)+""","""+str(numnewresult)+""","""+str(numnewflag)+""","""+str(numnewtests)+""","""+str(1)+""");""")
    db.commit()
    
    ############## INSERT DATA IN REAL DASHBOARD DB #############
    logging.warning("Inserting new data in production database...")
    #print("Inserting new data in production database...")
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_patient` (`id`,`pid`,`birthday`,`sex`,`location`) select `id`+"""+str(numcurpatient)+""",`pid`,`birthday`,`sex`,`location` from `dashboardtemp`.`dms_patient`;""")
    db.commit()
    # added to pid the number of the current patient records in order to correctly pair the sample with its patient.
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_sample` (`id`,`sid`,`tube_type`,`sample_type`,`pid`,`priority`,`create_datetime`) select `id`+"""+str(numcursample)+""",`sid`,`tube_type`,`sample_type`,`pid`+"""+str(numcurpatient)+""",`priority`,`create_datetime` from `dashboardtemp`.`dms_sample`;""")
    db.commit()
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_requested_tests` (`id`,`sid`,`test_name`,`create_datetime`,`action_code`) select `id`+"""+str(numcurreqtest)+""",`sid`,`test_name`,`create_datetime`,`action_code` from `dashboardtemp`.`dms_requested_tests`;""")
    db.commit()
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_result` (`id`,`sid`,`instrument_id`,`test_name`,`time_stamp`,`dilution_factor`,`result`,`aspect`,`flagged`,`auto_val_result`) select `id`+"""+str(numcurresult)+""",`sid`,`instrument_id`,`test_name`,`time_stamp`,`dilution_factor`,`result`,`aspect`,`flagged`,`auto_val_result` from `dashboardtemp`.`dms_result`;""")
    db.commit()
    # added to rid the number of the current result records in order to correctly pair the flag with its result.
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_flag` (`id`,`code`,`rid`) select `id`+"""+str(numcurflag)+""",`code`,`rid`+"""+str(numcurresult)+""" from `dashboardtemp`.`dms_flag`;""")
    db.commit()    
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_tests` (`id`,`test_name`,`test_code_las`) select `id`+"""+str(numcurtests)+""",`test_name`,`test_code_las` from `dashboardtemp`.`dms_tests`;""")
    db.commit()
    logging.warning("Insertion complete...")
    #print("Insertion complete...")
    
    ############## CONSISTENCY CONTROL #############
    logging.warning("Consistency control...")
    #print("Consistency control...")
    logging.warning("After new data insertion: ")
    #print("After new data insertion: ")
    # Current number of entries in the production database after new data insertion
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    numcurpatient2=int(cur.fetchone()[0])
    logging.warning("Current number of dms patient records in production dashboard database: " + str(numcurpatient2))
    #print("Current number of dms patient records in production dashboard database: " + str(numcurpatient2))
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    numcursample2=int(cur.fetchone()[0])
    logging.warning("Current number of dms sample records in production dashboard database: " + str(numcursample2))
    #print("Current number of dms sample records in production dashboard database: " + str(numcursample2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    numcurreqtest2=int(cur.fetchone()[0])
    logging.warning("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest2))
    #print("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_result`;""")
    numcurresult2=int(cur.fetchone()[0])
    logging.warning("Current number of dms result records in production dashboard database: " + str(numcurresult2))
    #print("Current number of dms result records in production dashboard database: " + str(numcurresult2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    numcurflag2=int(cur.fetchone()[0])
    logging.warning("Current number of dms flag records in production dashboard database: " + str(numcurflag2))
    #print("Current number of dms flag records in production dashboard database: " + str(numcurflag2))  
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    numcurtests2=int(cur.fetchone()[0])
    logging.warning("Current number of dms tests records in production dashboard database: " + str(numcurtests2))
    #print("Current number of dms tests records in production dashboard database: " + str(numcurtests2))  
    
    #Current max id number for each table
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    maxpatientid=int(cur.fetchone()[0])
    logging.warning("Current max id for table patient in production dashboard database: " + str(maxpatientid))
    #print("Current max id for table patient in production dashboard database: " + str(maxpatientid))
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    maxsampleid=int(cur.fetchone()[0])
    logging.warning("Current max id for table sample in production dashboard database: " + str(maxsampleid))
    #print("Current max id for table sample in production dashboard database: " + str(maxsampleid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    maxreqtestid=int(cur.fetchone()[0])
    logging.warning("Current max id for table requested tests in production dashboard database: " + str(maxreqtestid))
    #print("Current max id for table requested tests in production dashboard database: " + str(maxreqtestid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_result`;""")
    maxresultid=int(cur.fetchone()[0])
    logging.warning("Current max id for table result in production dashboard database: " + str(maxresultid))
    #print("Current max id for table result in production dashboard database: " + str(maxresultid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    maxflagid=int(cur.fetchone()[0])
    logging.warning("Current max id for table flag in production dashboard database: " + str(maxflagid))
    #print("Current max id for table flag in production dashboard database: " + str(maxflagid))  
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    maxtestsid=int(cur.fetchone()[0])
    logging.warning("Current max id for table tests in production dashboard database: " + str(maxtestsid))
    #print("Current max id for table tests in production dashboard database: " + str(maxtestsid))      
    
    errorflag = 0
    if numcurpatient2==numcurpatient+numnewpatient:
        logging.warning("All new patient data inserted")
        #print("All new patient data inserted")
    else:
        logging.warning("Some new patient record has not been inserted")
        #print("Some new patient record has not been inserted")
        errorflag=1
        
    if numcursample2==numcursample+numnewsample:
        logging.warning("All new sample data inserted")
        #print("All new sample data inserted")
    else:
        logging.warning("Some new sample record has not been inserted")
        #print("Some new sample record has not been inserted")
        errorflag=1
        
    if numcurreqtest2==numcurreqtest+numnewreqtest:
        logging.warning("All new requested tests data inserted")
        #print("All new requested tests data inserted")
    else:
        logging.warning("Some new requested tests record has not been inserted")
        #print("Some new requested tests record has not been inserted")
        errorflag=1
        
    if numcurresult2==numcurresult+numnewresult:
        logging.warning("All new result data inserted")
        #print("All new result data inserted")
    else:
        logging.warning("Some new result record has not been inserted")
        #print("Some new result record has not been inserted")    
        errorflag=1
        
    if numcurflag2==numcurflag+numnewflag:
        logging.warning("All new flag data inserted")
        #print("All new flag data inserted")
    else:
        logging.warning("Some new flag record has not been inserted")
        #print("Some new flag record has not been inserted")
        errorflag=1

    if numcurtests2==numcurtests+numnewtests:
        logging.warning("All new tests data inserted")
        #print("All new tests data inserted")
    else:
        logging.warning("Some new tests record has not been inserted")
        #print("Some new tests record has not been inserted")
        errorflag=1
        
# consistency check on the field id    
    if numcurpatient2==maxpatientid:
        logging.warning("consistency check on the field dms_patient.id OK")
        #print("consistency check on the field dms_patient.id OK")
    else:
        logging.warning("consistency check on the field dms_patient.id FAILED")
        #print("consistency check on the field dms_patient.id FAILED")
        errorflag=1
        
    if numcursample2==maxsampleid:
        logging.warning("consistency check on the field dms_sample.id OK")
        #print("consistency check on the field dms_sample.id OK")
    else:
        logging.warning("consistency check on the field dms_sample.id FAILED")
        #print("consistency check on the field dms_sample.id FAILED")
        errorflag=1        
    if numcurreqtest2==maxreqtestid:
        logging.warning("consistency check on the field dms_requested_tests.id OK")
        #print("consistency check on the field dms_requested_tests.id OK")
    else:
        logging.warning("consistency check on the field dms_requested_tests.id FAILED")
        #print("consistency check on the field dms_requested_tests.id FAILED")
        errorflag=1
    if numcurresult2==maxresultid:
        logging.warning("consistency check on the field dms_result.id OK")
        #print("consistency check on the field dms_result.id OK")
    else:
        logging.warning("consistency check on the field dms_result.id FAILED")
        #print("consistency check on the field dms_result.id FAILED")    
        errorflag=1  
    if numcurflag2==maxflagid:
        logging.warning("consistency check on the field dms_flag.id OK")
        #print("consistency check on the field dms_flag.id OK")
    else:
        logging.warning("consistency check on the field dms_flag.id FAILED")
        #print("consistency check on the field dms_flag.id FAILED")
        errorflag=1
    if numcurtests2==maxtestsid:
        logging.warning("consistency check on the field dms_tests.id OK")
        #print("consistency check on the field dms_tests.id OK")
    else:
        logging.warning("consistency check on the field dms_tests.id FAILED")
        #print("consistency check on the field dms_tests.id FAILED")
        errorflag=1
        
    ############## UPDATE NEW LINE IN DMS_UPLOAD_LOG AND DELETE NEW DATA IF ERROR OCCURRED############
    
    if errorflag==0:
        cur.execute("""UPDATE `"""+ realdashboarddb +"""`.`dms_upload_log` set uploaderror = 0  WHERE id = (select maxID from (SELECT MAX(id) maxID FROM `"""+ realdashboarddb +"""`.`dms_upload_log`) as t);""")
        db.commit()
        logging.warning("log updated without error")
        #print("log updated without error")
        cur.execute("""select ToDMSProcess from `"""+syncdatasourcedb+"""`.`"""+syncdatasourcetable+"""` where id="""+str(syncdatasourcetableid)+""";""")
        ToDMSProcess=cur.fetchone()
        if ToDMSProcess!=None:
            ToDMSProcess=str(ToDMSProcess[0])
        else:
            ToDMSProcess='0-0'
        updatedatasourcetable(db,syncdatasourcedb,syncdatasourcetable,syncdatasourcetableid,cur,2,str(int(ToDMSProcess.split('-')[0]))+"-"+str(int(ToDMSProcess.split('-')[0])),'work succesful')
        logging.warning("SYNC_DATASOURCE updated")
        #print("SYNC_DATASOURCE updated")
    else:
        logging.warning("log updated with error. Please change the file and repeat the procedure")
        #print("log updated with error. Please change the file and repeat the procedure")
        logging.warning("deleting new data...")
        #print("deleting new data...")
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_patient` where `id`>"""+str(numcurpatient)+""";""")
        db.commit()
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_sample` where `id`>"""+str(numcursample)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_requested_tests` where `id`>"""+str(numcurreqtest)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_result` where `id`>"""+str(numcurresult)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_flag` where `id`>"""+str(numcurflag)+""";""")
        db.commit() 
        cur.execute("""delete from`"""+ realdashboarddb +"""`.`dms_tests` where `id`>"""+str(numcurtests)+""";""")
        db.commit() 
        logging.warning("new data completely deleted")
        #print("new data completely deleted")
        cur.execute("""select ToDMSProcess from `"""+syncdatasourcedb+"""`.`"""+syncdatasourcetable+"""` where id="""+str(syncdatasourcetableid)+""";""")
        ToDMSProcess=cur.fetchone()
        if ToDMSProcess!=None:
            ToDMSProcess=str(ToDMSProcess[0])
        else:
            ToDMSProcess='0-0'
        updatedatasourcetable(db,syncdatasourcedb,syncdatasourcetable,syncdatasourcetableid,cur,-1,str(int(ToDMSProcess.split('-')[0])+1)+"-"+str(int(ToDMSProcess.split('-')[0])+1),'job failed')
        logging.warning("SYNC_DATASOURCE updated")
        #print("SYNC_DATASOURCE updated")
    
    ############## CANCEL DMS UNZIPPED FILE, TEMPORARY DASHBOARD DB AND TEMPORARY DMS DB ##############
    os.remove(unzipfolder+file[0])
    logging.warning('unzipped file removed')
    #print('unzipped file removed') 
    cur.execute("drop database dashboardtemp")
    cur.execute("drop database dmstemp")
    logging.warning('Temporary database removed')
    #print('Temporary database removed')    
    
    cur.close() 
    ############## CLOSE DB CONNECTION
    
    db.close()
    
if __name__=="__main__":
    main()
    
