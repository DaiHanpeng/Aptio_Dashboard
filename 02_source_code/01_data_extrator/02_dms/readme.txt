Aptio Dashboard DMS data extractor

prerequisites:
	\*using python 2.7.15
	sudo apt install python-pip
	sudo apt-get install build-essential python-dev libmysqlclient-dev
	sudo apt-get install python-mysqldb
	pip install MySQL-python
	pip install zipfile36
	pip install configparser
	install mysql-server and configured it:
		
		run: sudo apt update
		run: sudo apt install mysql-server
		run: sudo mysql_secure_installation
		\*In order to use a password to connect to MySQL as root, you will need to switch its authentication method from auth_socket to mysql_native_password. To do this, open up the MySQL prompt from your terminal:
		run: sudo mysql
		\*Next, check which authentication method each of your MySQL user accounts use with the following command:
		run: SELECT user,authentication_string,plugin,host FROM mysql.user;
		run: ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'lINEA_3';
		run: FLUSH PRIVILEGES
		

TODO

@system admin:

1 - please, remove the auto_increment from dms tables

	mysql> ALTER TABLE dms_result DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

	mysql> ALTER TABLE dms_tests DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

	mysql> ALTER TABLE dms_sample DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

	mysql> ALTER TABLE dms_requested_tests DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

	mysql> ALTER TABLE dms_patient DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

	mysql> ALTER TABLE dms_flag DROP PRIMARY KEY, CHANGE id id int(11), ADD PRIMARY KEY (id);

2 - Change the var type of dms_flag code to something longer than char24:

	ALTER TABLE dms_flag MODIFY code text;

3 - create dms log table

	CREATE TABLE `dms_upload_log` (
	  `id` int(11) NOT NULL AUTO_INCREMENT,
	  `filename` varchar(64) DEFAULT NULL COMMENT 'name of the uploaded file',
	  `npatient` int(16) DEFAULT NULL COMMENT 'number of new patient uploaded',
	  `nsample` int(16) DEFAULT NULL COMMENT 'number of new sample uploaded',
	  `nreqtest` int(16) DEFAULT NULL COMMENT 'number of new reqtest uploaded',
	  `nresult` int(16) DEFAULT NULL COMMENT 'number of new result uploaded',
	  `nflag` int(16) DEFAULT NULL COMMENT 'number of new flag uploaded',
	  `ntests` int(16) DEFAULT NULL COMMENT 'number of new tests uploaded',
	  `uploaderror` varchar(4) DEFAULT NULL COMMENT 'if upload failed value is 1, if not value is 0',
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `id_UNIQUE` (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;

4 - add to my.cnf the following:
	[mysqld]	sql_mode="STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	
	
TO RUN THE SCRIPT:
- change the parameters inside "dashconf.ini" file according to needs.
- keep the files "aptio_temp_dashboard.sql" and "dashconf.ini" in the same folder of the script.
- run the script with two arguments: the first is the path and the name of the dms backup file to uplaod, the second is the name of the production database for the specific customer/hospital/lab