#!/usr/bin/python
import MySQLdb
import _mysql_exceptions
import sys
import zipfile
import os
import re

# function to insert dms tables from backup sql file to temporary dms db
 
def insertdmstable(dmstable,dmssqlfilepath,mycursor):
    statement = ""
    linenum=0
    line_start=0
    line_end=0
    for line in open(dmssqlfilepath):
        linenum=linenum+1
        if re.search('-- Table structure for table `'+dmstable+'`', line):
            print("Process table starts on line number ",linenum)
            line_start=linenum
        if re.search('-- Table structure for table', line) and line_start!=0 and line_end==0 and linenum>line_start:
            print("Process table ends on line number ",linenum)
            line_end=linenum
    
        if  line_start!=0 and line_end==0:
            if re.match(r'--', line):  # ignore sql comment lines
                continue
            if not re.search(r";", line):  # keep appending lines that don't end in ';'
                statement = statement + line
            else:  # when you get a line ending in ';' then exec statement and reset for next statement
                statement = statement + line
                try:
                    mycursor.execute(statement)
                    #print(statement)
                except Exception as e:
                    print (e)
                    print("affected line of dms sql file is: ", linenum)
                    print(statement)
                    exit()
                statement = ""    
    print('Table '+dmstable+' processed')



def main():

    ############## SCRIPT PATAMETERS #############
    mysqlhostip="localhost"
    mysqluser="root"
    mysqlpw="lINEA_3"
    dmsbackuppw="L@inea-3"
    dashboarddb="../Aptio_Dashboard/03_database/aptio_dashboard.sql"
    unzipfolder="tmp/"
    realdashboarddb="dashboardtemp2"
    
    ############## CHECK SCRIPT ARGUMENTS ###############
    if (len(sys.argv) != 4):
        print("Arguments should be 3: 1*the full path of the file to process; 2*result status table; 3*id of the hospital DB --> Exit")
        sys.exit(0)
    
    print("This is the name of the file to process: ", sys.argv[1])
    print("This is the result status table: ", sys.argv[2])
    print("This is the id of the hospital: " , sys.argv[3])
    
    zipfilepath=sys.argv[1]
    tablefieldcomplete=sys.argv[2]
    dbid=sys.argv[3]
    
    
    ############## OPEN DMS BACKUP FILE ##########
    with zipfile.ZipFile(zipfilepath) as zf:
        zf.extractall(pwd=dmsbackuppw)
    
    ############## OPEN MYSQL CONNECTION ##############
    db = MySQLdb.connect(host=mysqlhostip,  # your host 
                     user=mysqluser,       # username
                     passwd=mysqlpw)     # password
    cur = db.cursor()
    
    ############## CHECK LAST UPLOAD STATUS AND REMOVE LAST INSERTED DATA IN CASE OF ERROR ##############
    flagconsistent=None
    #select uploaderror from `"""+realdashboarddb+"""`.`dms_upload_log` where id=(select max(id) from """+realdashboarddb+""".`dms_upload_log`);
    cur.execute("""select `uploaderror` from `"""+realdashboarddb+"""`.`dms_upload_log` where id=(select max(id) from """+realdashboarddb+""".`dms_upload_log`);""")
    flagconsistent=cur.fetchone()
    
    if flagconsistent!=None:
        #print(flagconsistent)
        flagconsistent=int(flagconsistent[0])
    else:
        flagconsistent=0
    if flagconsistent==0:
        print("Last updload completed succesfully. Now continuing with new data.")
    else:
        print("The last upload ended with error. Rolling back to the last consistent status...")
        cur.execute("""select sum(npatient) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnpatient=int(cur.fetchone()[0])
        cur.execute("""select sum(nsample) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnsample=int(cur.fetchone()[0])
        cur.execute("""select sum(nreqtest) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnreqtest=int(cur.fetchone()[0])
        cur.execute("""select sum(nresult) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnresult=int(cur.fetchone()[0])
        cur.execute("""select sum(nflag) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastnflag=int(cur.fetchone()[0])
        cur.execute("""select sum(ntests) from `"""+realdashboarddb+"""`.`dms_upload_log` where uploaderror=0;""")
        lastntests=int(cur.fetchone()[0])        
        print("deleting new data...")
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
        print("last consistent status reloaded")
    
    ############## CREATE temporary dashboard DB ###############
    try:
        print("Creating temporary dashboard database...")
        cur.execute("create database dashboardtemp")
        print("temporary dashboard database created")
        cur.execute("use dashboardtemp")
        with open(dashboarddb) as f:
            cur.execute(f.read().decode('utf-8'), multi=True)
        print("temporary dashboard database filled")
    except _mysql_exceptions.ProgrammingError as prog_error:
        print(prog_error[1])
        print('deleting database...')
        cur.execute("drop database dashboardtemp")
        print('Deletion complete')
        cur.execute("create database dashboardtemp")
        print("temporary dashboard database created")
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
                    print (e)
                    print("affected line of dashboard sql file is: ", linenum)
                    print(statement)
                statement = ""    
        print("temporary dashboard database filled")
    
    ############## CREATE temporary dms DB ###############    
    
    # run SET commands at the beginning of dms sql backup file
    try:
        print("Creating temporary dms database...")
        cur.execute("create database dmstemp")
        print("temporary dms database created")
        cur.execute("use dmstemp")        
    except _mysql_exceptions.ProgrammingError as prog_error:
        print(prog_error[1])
        print('deleting database...')
        cur.execute("drop database dmstemp")
        print('Deletion complete')
        cur.execute("create database dmstemp")
        print("temporary dms database created")
        cur.execute("use dmstemp")
    file=os.listdir(unzipfolder)
    print("processing: ", file[0])
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
                print "\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args))
   
    # insert anagpatient, anagpatient_details, reqtest, orders, reqtestresult, testinstrument and reqtube into the dms temporary database;
    dmstables=['anagpatient', 'anagpatient_details', 'reqtest', 'orders', 'reqtestresult', 'testinstrument', 'reqtube','eventautomation']
    
    for i in dmstables:
        insertdmstable(i,unzipfolder+file[0],cur)

    ############### INSERT data in temporary dashboard DB ##################
    
    print("transmitting data from temporary DMS database to temporary Dashboard database... ")
    
    #dms_patient
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_patient` (`pid`,`birthday`,`sex`,`location`) select `codpid`,`datbirth`,`flgsex`,`codlocation` from `dmstemp`.`anagpatient` as a inner join `dmstemp`.`anagpatient_details` as b on a.`codaid`=b.`codaid`""")
        db.commit()
    except Exception as e:
        print()
    
    #dms_sample
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_sample` (`sid`,`tube_type`,`sample_type`,`pid`,`priority`,`create_datetime`) select a.`codsid`,a.`valdiameter`,a.`codtypesample`,d.`id`,a.`flgstat`,b.`datorder` from `dmstemp`.`reqtube` as a inner join `dmstemp`.`orders` as b on a.`codoid`=b.`codoid` inner join `dmstemp`.`anagpatient` as c on b.`codaid`=c.`codaid` inner join `dashboardtemp`.`dms_patient` as d on c.`codpid`=d.`pid`""")
        db.commit()
    except Exception as e:
        print()
        
    #dms_requested_test
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_requested_tests` (`sid`,`test_name`,`create_datetime`,`action_code`) select a.`codsid`,a.`codtest`,b.`datorder`,a.`codactioncode` from `dmstemp`.`reqtest` as a inner join `dmstemp`.`orders` as b on a.`codoid`=b.`codoid`""")
        db.commit()
    except Exception as e:
        print()
        
    #dms_result
    try:
        cur.execute("""alter table dashboardtemp.dms_result add flagvalue text;""")
        #version without TAT
        #cur.execute("""insert ignore into `dashboardtemp`.`dms_result` (`sid`,`instrument_id`,`test_name`,`time_stamp`,`dilution_factor`,`result`,`aspect`,`flagged`,`flagvalue`,`auto_val_result`) select a.`codsid`,a.`codinstrument`,a.`codtest`,a.`datresult`,a.`valdilution`,a.`valresult1`,b.`validentifier1`,IF(a.`jsnflaginstrument`!="[]",1,0),a.`jsnflaginstrument`,a.`flgvalid` from `dmstemp`.`reqtestresult` as a inner join `dmstemp`.`testinstrument` as b on a.`codtest`=b.`codtest` where b.`codiid`!='HOSTASTMmCH' and b.`codiid`!='INPECO'""")
        #version with TAT
        cur.execute("""insert ignore into `dashboardtemp`.`dms_result` (`sid`,`instrument_id`,`test_name`,`time_stamp`,`dilution_factor`,`result`,`aspect`,`flagged`,`flagvalue`,`auto_val_result`,`tat_inlabbing_result`)  select a.`codsid`,a.`codinstrument`,a.`codtest`,a.`datresult`,a.`valdilution`,a.`valresult1`,b.`validentifier1`,IF(a.`jsnflaginstrument`!="[]",1,0),a.`jsnflaginstrument`,a.`flgvalid`,c.`tat` from `dmstemp`.`reqtestresult` as a inner join `dmstemp`.`testinstrument` as b on a.`codtest`=b.`codtest` inner join (select d.codsid,d.codtest,timediff(d.datresult,e.datcheckin) as tat from `dmstemp`.`reqtestresult` as d inner join (select codinstrument,min(datevent) as datcheckin,txtevent,codsid from `dmstemp`.`eventautomation` where txtevent='L001' group by codsid) as e on d.codsid=e.codsid) as c on a.`codsid`=c.`codsid` and a.`codtest`=c.`codtest` where b.`codiid`!='HOSTASTMmCH' and b.`codiid`!='INPECO';""")
        db.commit()
    except Exception as e:
        print()
    
    #dms_flag
    try:
        cur.execute("""ALTER TABLE `dashboardtemp`.`dms_flag` MODIFY `code` text;""")
        cur.execute("""insert ignore into `dashboardtemp`.`dms_flag` (`code`,`rid`) select `flagvalue`,`id` from `dashboardtemp`.`dms_result` where `flagvalue`!="[]" and `flagvalue`!="";""")
        db.commit()
    except Exception as e:
        print()

    #dms_tests
    try:
        cur.execute("""insert ignore into `dashboardtemp`.`dms_tests` (`test_name`,`test_code_las`) select `codtest`,`codtestsend` from `dmstemp`.`testinstrument` where `codiid`='INPECO';""")
        db.commit()
    except Exception as e:
        print()
    
    ############## COLLECT REAL DASHBOARD DB CURRENT STATUS AND NUMBER OF NEW DATA TO INSERT################
    # Current number of entries in the production database
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    numcurpatient=int(cur.fetchone()[0])
    print("Current number of dms patient records in production dashboard database: " + str(numcurpatient))
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    numcursample=int(cur.fetchone()[0])
    print("Current number of dms sample records in production dashboard database: " + str(numcursample))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    numcurreqtest=int(cur.fetchone()[0])
    print("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_result`;""")
    numcurresult=int(cur.fetchone()[0])
    print("Current number of dms result records in production dashboard database: " + str(numcurresult))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    numcurflag=int(cur.fetchone()[0])
    print("Current number of dms flag records in production dashboard database: " + str(numcurflag))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    numcurtests=int(cur.fetchone()[0])
    print("Current number of dms tests records in production dashboard database: " + str(numcurtests))    

    # Number of new entries to insert
    cur.execute("""select count(*) from `dashboardtemp`.`dms_patient`;""")
    numnewpatient=int(cur.fetchone()[0])
    print("number of dms patient records to insert in production dashboard database: " + str(numnewpatient))
    cur.execute("""select count(*) from `dashboardtemp`.`dms_sample`;""")
    numnewsample=int(cur.fetchone()[0])
    print("number of dms sample records to insert in production dashboard database: " + str(numnewsample))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_requested_tests`;""")
    numnewreqtest=int(cur.fetchone()[0])
    print("number of dms requested test records to insert in production dashboard database: " + str(numnewreqtest))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_result`;""")
    numnewresult=int(cur.fetchone()[0])
    print("number of dms result records to insert in production dashboard database: " + str(numnewresult))    
    cur.execute("""select count(*) from `dashboardtemp`.`dms_flag`;""")
    numnewflag=int(cur.fetchone()[0])
    print("number of dms flag records to insert in production dashboard database: " + str(numnewflag))
    cur.execute("""select count(*) from `dashboardtemp`.`dms_tests`;""")
    numnewtests=int(cur.fetchone()[0])
    print("number of dms tests records to insert in production dashboard database: " + str(numnewtests))       
    
    ############## INSERT NEW LINE IN DMS_UPLOAD_LOG ############
    print("Inserting new log line in production database...")
    cur.execute("""insert ignore into `"""+ realdashboarddb +"""`.`dms_upload_log` (`filename`,`npatient`,`nsample`,`nreqtest`,`nresult`,`nflag`,`ntests`,`uploaderror`) values ('"""+str(zipfilepath)+"""',"""+str(numnewpatient)+""","""+str(numnewsample)+""","""+str(numnewreqtest)+""","""+str(numnewresult)+""","""+str(numnewflag)+""","""+str(numnewtests)+""","""+str(1)+""");""")
    db.commit()
    
    ############## INSERT DATA IN REAL DASHBOARD DB #############
    print("Inserting new data in production database...")
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
    print("Insertion complete...")
    
    ############## CONSISTENCY CONTROL #############
    print("Consistency control...")
    print("After new data insertion: ")
    # Current number of entries in the production database after new data insertion
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    numcurpatient2=int(cur.fetchone()[0])
    print("Current number of dms patient records in production dashboard database: " + str(numcurpatient2))
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    numcursample2=int(cur.fetchone()[0])
    print("Current number of dms sample records in production dashboard database: " + str(numcursample2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    numcurreqtest2=int(cur.fetchone()[0])
    print("Current number of dms requested test records in production dashboard database: " + str(numcurreqtest2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_result`;""")
    numcurresult2=int(cur.fetchone()[0])
    print("Current number of dms result records in production dashboard database: " + str(numcurresult2))    
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    numcurflag2=int(cur.fetchone()[0])
    print("Current number of dms flag records in production dashboard database: " + str(numcurflag2))  
    cur.execute("""select count(*) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    numcurtests2=int(cur.fetchone()[0])
    print("Current number of dms tests records in production dashboard database: " + str(numcurtests2))  
    
    #Current max id number for each table
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_patient`;""")
    maxpatientid=int(cur.fetchone()[0])
    print("Current max id for table patient in production dashboard database: " + str(maxpatientid))
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_sample`;""")
    maxsampleid=int(cur.fetchone()[0])
    print("Current max id for table sample in production dashboard database: " + str(maxsampleid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_requested_tests`;""")
    maxreqtestid=int(cur.fetchone()[0])
    print("Current max id for table requested tests in production dashboard database: " + str(maxreqtestid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_result`;""")
    maxresultid=int(cur.fetchone()[0])
    print("Current max id for table result in production dashboard database: " + str(maxresultid))    
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_flag`;""")
    maxflagid=int(cur.fetchone()[0])
    print("Current max id for table flag in production dashboard database: " + str(maxflagid))  
    cur.execute("""select max(id) from `"""+realdashboarddb+"""`.`dms_tests`;""")
    maxtestsid=int(cur.fetchone()[0])
    print("Current max id for table tests in production dashboard database: " + str(maxtestsid))      
    
    errorflag = 0
    if numcurpatient2==numcurpatient+numnewpatient:
        print("All new patient data inserted")
    else:
        print("Some new patient record has not been inserted")
        errorflag=1
        
    if numcursample2==numcursample+numnewsample:
        print("All new sample data inserted")
    else:
        print("Some new sample record has not been inserted")
        errorflag=1
        
    if numcurreqtest2==numcurreqtest+numnewreqtest:
        print("All new requested tests data inserted")
    else:
        print("Some new requested tests record has not been inserted")
        errorflag=1
        
    if numcurresult2==numcurresult+numnewresult:
        print("All new result data inserted")
    else:
        print("Some new result record has not been inserted")    
        errorflag=1
        
    if numcurflag2==numcurflag+numnewflag:
        print("All new flag data inserted")
    else:
        print("Some new flag record has not been inserted")
        errorflag=1

    if numcurtests2==numcurtests+numnewtests:
        print("All new tests data inserted")
    else:
        print("Some new tests record has not been inserted")
        errorflag=1
        
# consistency check on the field id    
    if numcurpatient2==maxpatientid:
        print("consistency check on the field dms_patient.id OK")
    else:
        print("consistency check on the field dms_patient.id FAILED")
        errorflag=1
        
    if numcursample2==maxsampleid:
        print("consistency check on the field dms_sample.id OK")
    else:
        print("consistency check on the field dms_sample.id FAILED")
        errorflag=1        
    if numcurreqtest2==maxreqtestid:
        print("consistency check on the field dms_requested_tests.id OK")
    else:
        print("consistency check on the field dms_requested_tests.id FAILED")
        errorflag=1
    if numcurresult2==maxresultid:
        print("consistency check on the field dms_result.id OK")
    else:
        print("consistency check on the field dms_result.id FAILED")    
        errorflag=1  
    if numcurflag2==maxflagid:
        print("consistency check on the field dms_flag.id OK")
    else:
        print("consistency check on the field dms_flag.id FAILED")
        errorflag=1
    if numcurtests2==maxtestsid:
        print("consistency check on the field dms_tests.id OK")
    else:
        print("consistency check on the field dms_tests.id FAILED")
        errorflag=1
        
    ############## UPDATE NEW LINE IN DMS_UPLOAD_LOG AND DELETE NEW DATA IF ERROR OCCURRED############
    
    if errorflag==0:
        print()
        cur.execute("""UPDATE `"""+ realdashboarddb +"""`.`dms_upload_log` set uploaderror = 0  WHERE id = (select maxID from (SELECT MAX(id) maxID FROM `"""+ realdashboarddb +"""`.`dms_upload_log`) as t);""")
        db.commit()   
        print("log updated without error")
    else:
        print("log updated with error. Please change the file and repeat the procedure")
        print("deleting new data...")
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
        print("new data completely deleted")
     
    
    ############## CANCEL DMS UNZIPPED FILE, TEMPORARY DASHBOARD DB AND TEMPORARY DMS DB ##############
    # os.remove(unzipfolder+file[0])
    # print('unzipped file removed') 
    # cur.execute("drop database dashboardtemp")
    # cur.execute("drop database dmstemp")
    # print('Temporary database removed')    
    
    
    ############## CLOSE DB CONNECTION
    
    db.close()
    
if __name__=="__main__":
    main()
    