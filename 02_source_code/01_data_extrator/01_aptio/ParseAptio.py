import socket,time,os,sys,datetime,threading,json,pymysql,requests,gc,queue,zipfile

#--------基本配置----------
#配置文件名
configruation_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),'config.ini')
#日志记录路径
logpath = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),"Log")
#解析文件路径
filepath = ""
#数据库表主键列表
dbtablepri = {}
#医院id
project_id = ""
#一步法列表
one_step_methods = ['T4','FT4','T3','FT3','TSH3UL','aTG','aTPO','FSH','LH','TSTII','TSTO','PRL',
                    'PRGE','eE2','HCG','FOL','VB12','FER','BNP','AFP','CEA','CA125','PSA','cPSA',
                    'aHBe','BNP','CKMB','CARB','COR','CpS','DHEAS','DGTN','DIG','aHAVT','HBs','H2n',
                    'IRI','iPTH','MYO','PHTN','SYPH','THEO2','TnIUItra','IgE','VALP','VitaminD','ThCG']
#两步法列表
two_step_methods = ['CA153','CA199','aHAVM','aHBcM','HBcT','HCV','HBeAg','EHIV']
#-------------------------

#----------取日期及时间函数---------
def curtime(par):
    time1 = str(datetime.datetime.now())
    if par == "date":  #2017-01-01
        gc.collect()
        return time1[0:10]
    elif par == "datetime":  #2017-01-01 01:01:01
        gc.collect()
        return time1[0:19]
    elif par == "datetimemicrosec":  #2017-01-01 01:01:01.111
        gc.collect()
        return time1[0:23]
    elif par == "datetimed":  #20170101020202123
        gc.collect()
        return time1.replace("-","").replace(":","").replace(" ","").replace(".","")
    elif par == "second":
        gc.collect()
        return str(time.time()).split(".")[0]   #从1970年至今的秒数
    elif par == "secondms":
        gc.collect()
        return str(time.time())
    else:
        gc.collect()
        return ""
#-----------------------------------

#---------显示并记录日志文件---------
def printlog(text1,text2="",displaycontent=3,display=1,logtype="Main"):  #同步显示及记录日志文件,display:1界面显示，0为不显示，displaycontent:3=1+2，intodb强制插入数据库,1为强制，0为不强制
    try:
        os.mkdir(logpath)
    except:
        pass
    log_content = [curtime("datetimemicrosec")," ",str(text1),str(text2)]
    if display == 1:
        if displaycontent == 1:
            print (log_content[0]+log_content[1]+log_content[2])
        if displaycontent == 2:
            print (log_content[0]+log_content[1]+log_content[3])
        if displaycontent == 3:
            print (log_content[0]+log_content[1]+log_content[2]+log_content[3])
    try:
        logfile = open(logpath+"/"+logtype+"_"+curtime("date")+'.txt','a+')
        logfile.write(log_content[0]+log_content[1]+log_content[2]+log_content[3]+'\n')
        logfile.close()
    except Exception as err:
        pass
    if ('log_content' in dir()):
        del log_content
    if ('logfile' in dir()):
        del logfile
    gc.collect()
#------------------------------------

#----------2.2.3.1数据库函数----------
#查询对应数据是否存在，不存在则插入，condition，insertfield，insertvalue均为array
#condition = [['field1','=','value1'],['field2','>','value2'],['field3','<','value3'],....]
#insertfield = ["field1","field2","field3",...]
#insertvalue = ["value1","value2","value3",...]
#par代表需不需要先选择查询是否存在记录,insert代表不需要,select代表需要
#par2为是否需要update,0为需要,1为不需要，
#insertswitch为在查询记录为0的情况下是否需要插入,0为需要,1为不需要
#batchinsert是否使用批量插入方式
def dbinsert(conn,tablename,condition,insertfield,insertvalue,par,par2=0,insertswitch=0,batchinsert=False):
    global dbtablepri,updatetablelist
    exeresult = "Nothing"
    try:
        cur = conn.cursor()
        try:
            if par == "select":
                totalcondition = ""
                for everycondition in condition:
                    totalcondition = totalcondition+str(everycondition[0])+" "+str(everycondition[1])+" '"+str(everycondition[2])+"' and "
                if ('everycondition' in dir()):
                    del everycondition
                tmppri = dbtablepri[tablename]
                selectcol = ""
                for onepri in tmppri:
                    selectcol = selectcol + onepri + ","
                selectcol = selectcol[0:-1]
                addcondition = ""
                if len(tmppri) == 1:
                    if tmppri[0] == "id" or tmppri[0] == "central_id":
                        addcondition = str(tmppri[0])+" >= '0' and "
                sql = "select "+selectcol+" from "+tablename+" where "+addcondition+totalcondition[0:-5]
                sql = sql.replace("= 'None'","is NULL")
                
                try:
                    cur.execute(sql)
                    result = cur.fetchall()
                except Exception as err:
                    result = ""
                    printlog("--2.2.3.1 SQL 'Select' Excute Fail: "+str(err))
                    printlog("--2.2.3.1 SQL = "+str(sql))
                if ('totalcondition' in dir()):
                    del totalcondition
                if ('addcondition' in dir()):
                    del addcondition
                if ('sql' in dir()):
                    del sql
                if ('onepri' in dir()):
                    del onepri
                if ('selectcol' in dir()):
                    del selectcol
            elif par == "insert":
                result = ""
            if len(result) == 0:
                if insertswitch == 0:
                    if batchinsert == False:
                        sql = "insert into "+tablename+" ("+str(insertfield)[1:-2].replace("'","")+") values ("+str(insertvalue)[1:-1]+")"
                        sql = sql.replace("'null'","NULL")  #将空置为null
                        sql = sql.replace("'None'","NULL")  #将None置为null
                        try:
                            cur.execute(sql)
                            conn.commit()
                            exeresult = "Insert"
                        except Exception as err:
                            printlog("--2.2.3.1 SQL 'Insert' Excute Fail: "+str(err))
                            printlog("--2.2.3.1 SQL = "+str(sql))
                            conn.rollback()
                            exeresult = "InsertFail"
                        del sql
                    else:
                        exeresult = sqlbatchinsert(conn,cur,tablename,insertfield,insertvalue)
                else:
                    exeresult = "NoInsert"
            elif len(result) != 0 and par2 != 1:
                for onresult in range(len(result)):
                    tmpupdatecondition = ""
                    for onenum in range(len(tmppri)):
                        tmpupdatecondition = tmpupdatecondition + str(tmppri[onenum]) + " = '" + str(result[onresult][onenum]) + "',"
                    tmpupdatecondition = tmpupdatecondition[0:-1]
                    temp_condition = []
                    for k in condition:    #创建查询关键字列表
                        temp_condition.append(k[0])
                    if ('k' in dir()):
                        del k
                    temp_content = ""
                    for a in range(len(insertfield)):   #拼接插入值字段
                        if insertfield[a] not in temp_condition:    #如果set的值不在查询关键字列表中，则组合
                            temp_content = temp_content+str(insertfield[a])+" = '"+str(insertvalue[a])+"',"
                    if ('a' in dir()):
                        del a
                    sql = "update "+tablename+" set "+temp_content[0:-1]+" where "+tmpupdatecondition.replace("= 'None'","is NULL")  #条件将NONE替换为NULL
                    sql = sql.replace("'null'","NULL")  #将空置为null
                    sql = sql.replace("'None'","NULL")  #将None置为null
                    #print(sql)
                    try:
                        cur.execute(sql)
                        conn.commit()
                        exeresult = "Update"
                    except Exception as err:
                        printlog("--2.2.3.1 SQL 'Update' Excute Fail: "+str(err))
                        printlog("--2.2.3.1 SQL= "+str(sql))
                        conn.rollback()
                        exeresult = "UpdateFail"
                    del sql
                    if ('temp_condition' in dir()):
                        del temp_condition
                    if ('temp_content' in dir()):
                        del temp_content
                    if ('tmpupdatecondition' in dir()):
                        del tmpupdatecondition
            else:
                exeresult = "NoUpdate"
            if ('tmppri' in dir()):
                del tmppri
            if ('result' in dir()):
                del result
        except Exception as err:
            printlog("--2.2.3.1 SQL Excute Fail: "+str(err))
        try:
            cur.close()
        except Exception as err:
            printlog("--2.2.3.1 Close SQL Cursor Fail: "+str(err))
        if ('cur' in dir()):
            del cur
    except Exception as err:
        printlog("--2.2.3.1 Get SQL Cursor Fail: "+str(err))
    gc.collect()
    return exeresult
#------------------------------

#------------2.2.3.1.1批量insert----------
def sqlbatchinsert(conn,cur,tablename,insertfield,insertvalue):
    sql1 = "insert into "+tablename+" ("+str(insertfield)[1:-2].replace("'","")+") values "
    tmpvalue = ""
    for one in insertvalue:
        tmpvalue = tmpvalue + str(one).replace("[",'(').replace("]",')') + ','
        if len(tmpvalue) > sqllimit:
            sql = sql1+tmpvalue[0:-1]
            sql = sql.replace("'null'","NULL")  #将空置为null
            sql = sql.replace("'None'","NULL")  #将None置为null
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as err:
                printlog("--2.2.3.1.1 SQL 'Insert' Excute Fail: "+str(err))
                printlog("--2.2.3.1.1 SQL = "+str(sql))
                conn.rollback()
            tmpvalue = ""
    if len(tmpvalue) > 0:
        sql = sql1+tmpvalue[0:-1]
        sql = sql.replace("'null'","NULL")  #将空置为null
        sql = sql.replace("'None'","NULL")  #将None置为null
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as err:
            printlog("--2.2.3.1.1 SQL 'Insert' Left SQL Excute Fail: "+str(err))
            printlog("--2.2.3.1.1 SQL = "+str(sql))
            conn.rollback()
    if ('sql1' in dir()):
        del sql1
    if ('one' in dir()):
        del one
    if ('tmpvalue' in dir()):
        del tmpvalue
    if ('sql' in dir()):
        del sql
    gc.collect()
    return "Insert"
#-----------------------------------------

#-------获取指定前后字符串中的内容------
def getposinfo(targetstr,start,end):
    if int(targetstr.find(start)) >= 0 and int(targetstr.find(end,int(targetstr.find(start)))) >= 0:
        return targetstr[int(targetstr.find(start))+len(start):int(targetstr.find(end,int(targetstr.find(start))+len(start)))]
    else:
        return ""
#-------------------------------------

#--------------13 读取LAS LOG日志------------------
#-----------13.0 通用函数---------------
def calcresult(result,total):
    if result in list(total.keys()):
        total[result] = total[result] + 1
#--------------------------------------
#-----------13.1 拆分log每行内容----------
def getlasloglinecontent(text):
    return getposinfo(text,'timestamp="','"'),getposinfo(text,'sender="','"'),getposinfo(text,'receiver="','"'),\
           getposinfo(text,'message="','"'),getposinfo(text,'type="','"')
def getlaserrorloglinecontent(text):
    return getposinfo(text,'timestamp="','"'),getposinfo(text,'nodeID="','"'),getposinfo(text,'nodetype="','"'),\
           getposinfo(text,'errorcode="','"'),getposinfo(text,'sampleID="','"'),'Off-line='+getposinfo(text,'Off-line="','"')
def getprocesslogcontent(text):
    return getposinfo(text,'timestamp="','"'),getposinfo(text,'sampleID="','"'),getposinfo(text,'carrierID="','"'),\
           getposinfo(text,'nodeID="','"'),getposinfo(text,'processstep="','"'),getposinfo(text,'data="','"')
#------------------------------------
#-----------13.2 分析control日志-----------
def parselaslog(content,conn,onefilepath=""):
    msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
    for o in content:
        laslogtimestamp = ""
        node_id = ""
        node_type = ""
        sample_id = ""
        carrier_id = ""
        log_type = ""
        log_code = ""
        log_detail = ""
        add_error = ""
        if o.startswith("<Control"):
            try:
                laslogtimestamp,laslogsender,laslogreceiver,laslogmsg,laslogtype = getlasloglinecontent(o)
                msgtmplist = laslogmsg.split(" ")
                node_id = msgtmplist[0]
                if len(node_id) == 1:
                    node_id = '00'+node_id
                if len(node_id) == 2:
                    node_id = '0'+node_id
                node_type = msgtmplist[1]
                #根据不同数据类型执行不同函数
                try:
                    if laslogmsg.split(" ")[2] == "SAMPLE-DETECTED":
                        sample_id = msgtmplist[3].split("^")[1]
                        carrier_id = msgtmplist[3].split("^")[0]
                        log_type = "Sample_Route"
                        log_code = "DETECTED"
                        tmp_detail = msgtmplist[3].split("^")
                        add_error = msgtmplist[3].split("^")[2]
                        del tmp_detail[0]
                        del tmp_detail[0]
                        log_detail = ""
                        for y in tmp_detail:
                            log_detail = log_detail + "^" + str(y)
                        log_detail = log_detail[1:]
                except Exception as err:
                    printlog("--13.2 Parse DETECT Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "ADD":
                        sample_id = msgtmplist[3].split("|")[1]
                        carrier_id = msgtmplist[3].split("|")[0]
                        log_type = "Sample_Route"
                        log_code = "ADD"
                        log_detail = ""
                        add_error = ""
                except Exception as err:
                    printlog("--13.2 Parse ADD Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "DIVERTED":
                        sample_id = msgtmplist[3].split("^")[1]
                        carrier_id = msgtmplist[3].split("^")[0]
                        log_type = "Sample_Route"
                        log_code = "DIVERT"
                        log_detail = ""
                        add_error = msgtmplist[3].split("^")[2]
                except Exception as err:
                    printlog("--13.2 Parse DIVERTED Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "RETURNED":
                        sample_id = msgtmplist[3].split("^")[1]
                        carrier_id = msgtmplist[3].split("^")[0]
                        log_type = "Sample_Route"
                        log_code = "RETURN"
                        log_detail = ""
                        add_error = msgtmplist[3].split("^")[2]
                except Exception as err:
                    printlog("--13.2 Parse RETURNED Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "SAMPLE-LOCATION":
                        sample_id = msgtmplist[3].split("^")[1]
                        carrier_id = msgtmplist[3].split("^")[0]
                        log_type = "Sample_Route"
                        log_code = "LOCATION"
                        log_detail = msgtmplist[3]
                        add_error = msgtmplist[3].split("^")[2]
                except Exception as err:
                    printlog("--13.2 Parse LOCATION Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "SAMPLE-DISPOSED":
                        sample_id = msgtmplist[3].split("^")[1]
                        carrier_id = msgtmplist[3].split("^")[0]
                        log_type = "Sample_Route"
                        log_code = "DISPOSED"
                        log_detail = ""
                        add_error = msgtmplist[3].split("^")[2]
                except Exception as err:
                    printlog("--13.2 Parse DISPOSED Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "RACK-EVENT":
                        if len(msgtmplist[3].split("^")) > 2:
                            sample_id = msgtmplist[3].split("^")[3]
                            carrier_id = msgtmplist[3].split("^")[2]
                            log_type = "Rack_Event"
                            log_code = msgtmplist[3].split("^")[1]
                            log_detail = msgtmplist[3]
                            add_error = msgtmplist[3].split("^")[0]
                except Exception as err:
                    printlog("--13.2 Parse RACK-EVENT Fail: "+str(err))
                try:
                    if laslogmsg.split(" ")[2] == "LOAD-STATUS":
                        sample_id = msgtmplist[3].split("^")[0]
                        carrier_id = msgtmplist[3].split("^")[1]
                        log_type = "LOAD-STATUS"
                        log_code = "LOAD-STATUS"
                        log_detail = "Routine^STAT^Empty-"+msgtmplist[3]
                        add_error = msgtmplist[3].split("^")[2]
                except Exception as err:
                    printlog("--13.2 Parse LOAD-STATUS Fail: "+str(err))
            except Exception as err:
                printlog("--13.2 Parse Control Line Fail: "+str(err))
        if o.startswith("<Error"):
            try:
                laslogtimestamp,error_node_id,error_node_type,error_log_code,error_sample_id,error_log_detail=getlaserrorloglinecontent(o)
                node_id = error_node_id
                if len(node_id) == 1:
                    node_id = '00'+node_id
                if len(node_id) == 2:
                    node_id = '0'+node_id
                node_type = error_node_type.replace("DRM","IOM")
                log_type = "Error"
                log_code = error_log_code
                sample_id = error_sample_id
                log_detail = error_log_detail
                add_error = error_log_code
            except Exception as err:
                printlog("--13.2 Parse Error Line Fail: "+str(err))
        #对上方消息插库
        if log_type == "LOAD-STATUS" and sample_id.isdigit() and carrier_id.isdigit() and add_error.isdigit():  #如果是load-status消息,同时都是数字
                tablename = "aptio_load_status"
                condition = [["time_stamp","=",laslogtimestamp],["node_id","=",node_id],["node_type","=",node_type],\
                                ["log_type","=",log_type]]
                insertfield = ["time_stamp","node_id","node_type","log_type","log_details","routine_count","stat_count","empty_count"]
                insertvalues = [laslogtimestamp,node_id,node_type,log_type,log_detail,sample_id,carrier_id,add_error]
                execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
                calcresult(execresult,msgcount)
        else:  #如果有任何一个消息
            if sample_id or carrier_id or log_code or log_detail:
                tablename = "aptio_log"
                condition = [["node_id","=",node_id],["sample_id","=",sample_id],["carrier_id","=",carrier_id],["time_stamp","=",laslogtimestamp],\
                                ["node_type","=",node_type],["log_type","=",log_type],["log_code","=",log_code]]
                insertfield = ["node_id","sample_id","carrier_id","time_stamp","node_type","log_type","log_code","log_details",'log_error']
                insertvalues = [node_id,sample_id,carrier_id,laslogtimestamp,node_type,log_type,log_code,log_detail,add_error]
                execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
                calcresult(execresult,msgcount)
            #如果日志信息中有报错信息
            if add_error !="" and add_error != "0000" and log_type != "LOAD-STATUS":
                tablename = "aptio_log"
                condition = [["node_id","=",node_id],["sample_id","=",sample_id],["carrier_id","=",carrier_id],["time_stamp","=",laslogtimestamp],\
                                ["node_type","=",node_type],["log_type","=","Error"],["log_code","=",add_error]]
                insertfield = ["node_id","sample_id","carrier_id","time_stamp","node_type","log_type","log_code","log_details",'log_error']
                insertvalues = [node_id,sample_id,carrier_id,laslogtimestamp,node_type,"Error",add_error,log_detail,add_error]
                execresult =  dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
                calcresult(execresult,msgcount)
        if o.startswith("<ProcessStep"):
            try:
                pro_laslogtimestamp,pro_sample_id,pro_carrier_id,pro_node_id,pro_step,pro_data=getprocesslogcontent(o)
                if len(pro_node_id) == 1:
                    pro_node_id = '00'+pro_node_id
                if len(pro_node_id) == 2:
                    pro_node_id = '0'+pro_node_id
                tablename = "aptio_process"
                condition = [["time_stamp","=",pro_laslogtimestamp],["sample_id","=",pro_sample_id],["carrier_id","=",pro_carrier_id],\
                                ["node_id","=",pro_node_id],["process_step","=",pro_step],["data","=",pro_data]]
                insertfield = ["time_stamp","sample_id","carrier_id","node_id","process_step","data"]
                insertvalues = [pro_laslogtimestamp,pro_sample_id,pro_carrier_id,pro_node_id,pro_step,pro_data]
                execresult =  dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
                calcresult(execresult,msgcount)
            except Exception as err:
                printlog("--13.2 Parse Error Line Fail: "+str(err))
    printlog("--Processing Record Num: "+str(msgcount))
    gc.collect()
#------------------------------------
#-----------13.3解析layout文件-------
def parselayout(content,conn,onefilepath=""):
    msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
    for i in range(len(content)-1):
        tmplist = content[i+1].strip().split(";")
        node_id = tmplist[0]
        node_type = tmplist[2]
        node_name = tmplist[3] + "_" + tmplist[8]
        tablename = "aptio_node"
        condition = [["node_id","=",node_id],["node_type","=",node_type]]
        insertfield = ["node_id","node_type","node_name"]
        insertvalues = [node_id,node_type,node_name]
        execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
        calcresult(execresult,msgcount)
    printlog("--Processing Record Num: "+str(msgcount))
    gc.collect()
#------------------------------------
#-----------13.4解析nodetype文件-----
def parsenodetype(content,conn,onefilepath=""):
    msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
    for i in range(len(content)-1):
        tmplist = content[i+1].strip().split(";")
        node_type = tmplist[0]
        node_class = tmplist[2]
        node_capacity = tmplist[5]
        if node_capacity.isdigit():
            tablename = "aptio_node_config"
            condition = [["node_type","=",node_type]]
            insertfield = ["node_type","node_class","node_capacity"]
            insertvalues = [node_type,node_class,node_capacity]
            execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
        else:
            if node_capacity != "":
                printlog("--PrepareError: "+str(node_type)+": "+str(node_capacity))
            execresult = "PrepareError"
        calcresult(execresult,msgcount)
    printlog("--Processing Record Num: "+str(msgcount))
    gc.collect()
#------------------------------------
#-----------13.5解析testtable文件---------
def parsetestdef(content,conn,onefilepath=""):
    testcodefilepath = os.path.split(onefilepath)[0]+"\\Analyzer-Test-Codes.ini"
    if os.path.exists(testcodefilepath):
        if os.path.isfile(testcodefilepath):
            try:
                test_table = {}
                for i in range(len(content)-1):
                    tmplist = content[i+1].strip().split("\t")
                    astm_code = tmplist[0]
                    test_code = tmplist[1]
                    node_type = tmplist[3]
                    test_table[node_type+"|"+astm_code] = {"astm_code":astm_code,"test_code":test_code,"node_type":node_type}
                parsetestcodedef(testcodefilepath,conn,test_table)
            except Exception as err:
                printlog("--13.5 Parse test table file Fail: "+str(err))
        else:
            printlog("--13.5 Bad File Fail: Analyzer-Test-Codes.ini is not a file")
    else:
        printlog("--13.5 Missing File Fail: Analyzer-Test-Codes.ini")
    gc.collect()
#------------------------------------
#-----------13.6解析testcode文件------
def parsetestcodedef(codefilepath,conn,testcodedict):
    printlog ("--Processing File: Analyzer-Test-Codes.ini")
    try:
        codefile = open(codefilepath,"r")
        codefilecontent = codefile.readlines()   #读取文件
        codefile.close()   #关闭文件
        msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
        try:
            for i in range(len(codefilecontent)-1):
                tmplist = codefilecontent[i+1].strip().split("\t")
                node_type = tmplist[0]
                analyzer_code = tmplist[1]
                astm_code = tmplist[2]
                if node_type+"|"+astm_code in testcodedict.keys():
                    testcodedict[node_type+"|"+astm_code]["analyzer_code"] = analyzer_code
            printlog ("--Processing File: Analyzer-Test-Codes.ini Completed")
            for i in testcodedict:
                if "test_code" in testcodedict[i] and "astm_code" in testcodedict[i] and "analyzer_code" in testcodedict[i] and "node_type" in testcodedict[i]:
                    test_name = testcodedict[i]["test_code"]
                    test_code_lis = testcodedict[i]["astm_code"]
                    test_code_instrument = testcodedict[i]["analyzer_code"]
                    if test_code_instrument in one_step_methods:
                        immune_method = '1'
                    elif test_code_instrument in two_step_methods:
                        immune_method = '2'
                    else:
                        immune_method = '0'
                    analyzer_type = testcodedict[i]["node_type"]
                    is_sort_test = '0'
                    tablename = "aptio_tests"
                    condition = [["analyzer_type","=",analyzer_type],["test_name","=",test_name]]
                    insertfield = ["test_name","test_code_lis","test_code_instrument","analyzer_type","is_sort_test","immune_method"]
                    insertvalues = [test_name,test_code_lis,test_code_instrument,analyzer_type,is_sort_test,immune_method]
                    execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select")
                    calcresult(execresult,msgcount)
                else:
                    printlog("--13.6 Miss necessary column Fail: "+str(testcodedict[i]))
            printlog("--Processing Record Num: "+str(msgcount))
        except Exception as err:
            printlog("--13.6 Parse test code file Fail: "+str(err))
    except Exception as err:
        printlog("--13.6 Open File Fail: "+str(codefilepath)+" "+str(err))
    gc.collect()
#-----------13.7解析sorttest文件------
def parsesorttestdef(content,conn,onefilepath=""):
    msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
    for i in range(len(content)-1):
        tmplist = content[i+1].strip().split("\t")
        test_code_lis = tmplist[0]
        test_name = tmplist[1]
        test_code_instrument = ""
        analyzer_type = ""
        is_sort_test = '1'
        tablename = "aptio_tests"
        condition = [["test_name","=",test_name],["is_sort_test","=",is_sort_test]]
        insertfield = ["test_name","test_code_lis","test_code_instrument","analyzer_type","is_sort_test"]
        insertvalues = [test_name,test_code_lis,test_code_instrument,analyzer_type,is_sort_test]
        execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",1)
        calcresult(execresult,msgcount)
    printlog("--Processing Record Num: "+str(msgcount))
    gc.collect()
#------------------------------------
#------------13.8解析error文件--------
def parseerrorinidef(content,conn,onefilepath=""):
    conn = pymysql.connect(host=mysqlhost, port=mysqlport, user=mysqluser, passwd=mysqlpassword,db=mysqldb,charset='utf8')
    msgcount = {"Insert":0,"NoInsert":0,"InsertFail":0,"Update":0,"NoUpdate":0,"UpdateFail":0,"Nothing":0,"PrepareError":0}
    for i in range(len(content)-1):
        tmplist = content[i+1].strip().split("\t")
        node_type = tmplist[0]
        log_code = tmplist[1]
        log_message = tmplist[2].replace(","," ").replace("'",'"')
        log_detail = tmplist[3].replace(","," ").replace("'",'"')
        log_type = tmplist[6]
        tablename = "aptio_log_def"
        condition = [['node_type','=',node_type],['log_code','=',log_code]]
        insertfield = ['node_type','log_code','log_message','log_detail','log_type']
        insertvalues = [node_type,log_code,log_message,log_detail,log_type]
        execresult = dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",0)
        calcresult(execresult,msgcount)
    printlog("--Processing Record Num: "+str(msgcount))
    gc.collect()
#------------------------------------
#--------------------------------------------------
    
#--------------读取文件--------------
def readfile(onefilepath,onefilename,parsefunc,conn):
    begintime = curtime("secondms")
    printlog ("--Processing File: "+str(onefilename))
    try:
        onefile = open(onefilepath,"r",errors='ignore')
        onefilecontent = onefile.readlines()   #读取文件
        onefile.close()   #关闭文件
        parsefunc(onefilecontent,conn,onefilepath)
    except Exception as err:
        printlog("--13 Open File Fail: "+str(onefilepath)+" "+str(err))
    printlog ("--Processing File: "+str(onefilename)+" Complete, spend: "+str(float(curtime("secondms"))-float(begintime))+" s")
#-----------------------------------
#--------------主流程----------------
def mainparse(laslogpath):
    printlog("--Begin Parsing")
    logfilelist = ""
    process_num = 1
    try:
        conn = pymysql.connect(host=mysqlhost, port=mysqlport, user=mysqluser, passwd=mysqlpassword,db=mysqldb,charset='utf8')
        tablename = "sync_datasource"
        condition = [['id','=',project_id]]
        insertfield = ['ToAptioStatus']
        insertvalues = ['0']
        dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",0,insertswitch=1)
        #解压缩文件
        try:
            logfilelist = os.listdir(laslogpath)
            for i in logfilelist:
                if os.path.isfile(laslogpath+"\\"+i) and i.endswith(".zip"):
                    printlog("--Unzip File: "+str(i))
                    zp = zipfile.ZipFile(laslogpath+"\\"+i,'r')
                    zp.extractall(laslogpath)
                    zp.close()
        except Exception as err:
            printlog("--Unzip File Fail: "+str(err) + " filename: " + str(i))
        #执行主流程
        try:
            logfilelist = [(i, os.stat(os.path.join(laslogpath,i)).st_mtime) for i in os.listdir(laslogpath)]   #排序文件
        except Exception as err:
            printlog("--13 Get Filelist Fail: "+str(err)+" Path: "+str(laslogpath))
        if len(logfilelist) > 0 :  #如果成功获取到文件列表
            tablename = "sync_datasource"
            condition = [['id','=',project_id]]
            insertfield = ['ToAptioStatus']
            insertvalues = ['1']
            dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",0,insertswitch=1)
            for i in sorted(logfilelist, key=lambda x: x[1]):
                process_text = str(process_num)+"/"+str(len(logfilelist))
                printlog("--Parsing: "+process_text)
                tablename = "sync_datasource"
                condition = [['id','=',project_id]]
                insertfield = ['ToAptioProcess']
                insertvalues = [process_text]
                dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",0,insertswitch=1)
                onefilepath = os.path.join(laslogpath,i[0])
                if os.path.isfile(onefilepath):   #判断路径是不是文件
                    if (i[0].startswith("CONTROL") and (i[0].endswith(".TXT") or i[0].endswith(".XML"))) or\
                        (i[0].startswith("ERROR") and (i[0].endswith(".TXT") or i[0].endswith(".XML"))) or\
                        (i[0].startswith("PROCESS") and (i[0].endswith(".TXT") or i[0].endswith(".XML"))):
                        readfile(onefilepath,i[0],parselaslog,conn)
                    elif i[0] == "Layout.csv":
                        readfile(onefilepath,i[0],parselayout,conn)
                    elif i[0] == "Node-Types.csv":
                        readfile(onefilepath,i[0],parsenodetype,conn)
                    elif i[0] == "Test-Information.ini":
                        readfile(onefilepath,i[0],parsetestdef,conn)
                    elif i[0] == "Sorting-Tests.ini":
                        readfile(onefilepath,i[0],parsesorttestdef,conn)
                    elif i[0] == "Error-Handling.ini":
                        readfile(onefilepath,i[0],parseerrorinidef,conn)
                    else:
                        printlog("--Skip File: "+str(i[0]))
                else:
                    printlog("--Skip Folder: "+str(i[0]))
                process_num = process_num + 1
            tablename = "sync_datasource"
            condition = [['id','=',project_id]]
            insertfield = ['ToAptioStatus']
            insertvalues = ['2']
            dbinsert(conn,tablename,condition,insertfield,insertvalues,"select",0,insertswitch=1)
            try:
                conn.close()
            except Exception as err:
                printlog("--Disconnect to Database Fail: "+str(err))
    except Exception as err:
        printlog("--Connect to DB Fail: "+str(err))
    gc.collect()
    printlog("--Parsing Complete")
#--------------------------------------------------

#------读取配置文件参数-------
try:
    if os.path.exists(configruation_file):
        try:
            configfile = open(configruation_file,"r",encoding='utf-8')  #读取配置文件
            lines = configfile.readlines()  #读取文件所有内容
        except:
            configfile = open(configruation_file,"r",encoding='gbk')  #读取配置文件
            lines = configfile.readlines()  #读取文件所有内容
        finally:
            configfile.close()
        configdict = {}  #定义一个dict
        for line in lines:
            line = line.strip('\n')  #去除尾部换行
            if line:  #如果不是空行
                config = line.split("=")  #以“=”分隔
                if len(config) == 2 and line.startswith("#") == False:   #判断下数据list长度
                    parname = config[0].strip()  #取参数名字并去除前后空格
                    value = config[1].strip()    #取参数值并去除前后空格
                    configdict[parname] = value  #将值存入dict中
        readconfig = 1
    else:
        readconfig = 1
except Exception as err:
    printlog("--Read Configruation File Fail: "+str(err))
    readconfig = 0
gc.collect()
#-----------------------------

if readconfig == 1:
    #-------mysql参数-----------
    try:
        mysqlhost = configdict["mysqlhost"]
    except:
        mysqlhost = "127.0.0.1"
    try:
        mysqldb = configdict["mysqldb"]
    except:
        mysqldb = "aptio_dashboard"
    try:
        mysqlport = int(configdict["mysqlport"])
    except:
        mysqlport = 3306
    #---------------------------
    #--------读取数据库用户及密码--------
    try:
        mysqllocaluser = configdict["mysqllocaluser"]
    except:
        mysqllocaluser = "SYSinfo"
    try:
        mysqllocalpw = configdict["mysqllocalpw"]
    except:
        mysqllocalpw = "SYSinfo123!"
    mysqluser = mysqllocaluser
    mysqlpassword = mysqllocalpw
    #---------读取一步法两步法的列表----------
    try:
        one_step_methods_add = configdict["one_step_method"].split(",")
        one_step_methods = one_step_methods + one_step_methods_add
    except:
        pass
    try:
        two_step_methods_add = configdict["two_step_methods_add"].split(",")
        two_step_methods = two_step_methods + two_step_methods_add
    except:
        pass
    #-----------------------------------------
    #参数列表[当前文件路径，laslog文件路径，项目id，数据库名]
    parlist = sys.argv
    if len(parlist) >= 2:
        laslogfilepath = parlist[1]
        if len(parlist) >= 3:
            project_id = parlist[2]
        if len(parlist) >= 4:
            mysqldb = parlist[3]
        conn = pymysql.connect(host=mysqlhost, port=mysqlport, user=mysqluser, passwd=mysqlpassword,db=mysqldb,charset='utf8')
        cur = conn.cursor()
        try:   #获取数据库结果
            printlog("--Get DB schema")
            sql = "show tables;"
            cur.execute(sql)
            for one in cur.fetchall():
                sql = "desc "+str(one[0])
                cur.execute(sql)
                tablepri = []
                for oneone in cur.fetchall():
                    if oneone[3] == "PRI":
                        tablepri.append(str(oneone[0]))
                dbtablepri[one[0]] = tablepri
        except Exception as err:
                printlog("--Get DB schema Fail: "+str(err))
        cur.close()
        conn.close()
        mainparse(laslogfilepath)
    else:
        printlog("--Missing necessary parameter")
