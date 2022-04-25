-- Populate Table REGIONS
-------------------------
INSERT INTO REGIONS (REGION_ID, REGION_NAME) 
VALUES
  (1,'Europe'),
  (2,'Americas'),
  (3,'Asia'),
  (4,'Middle East and Africa')
;

-- Populate Table COUNTRIES
---------------------------
INSERT INTO COUNTRIES (COUNTRY_ID,COUNTRY_NAME,REGION_ID,DATE_LAST_UPDATED) 
VALUES
  ('AR','Argentina',2,'2021-06-13 12:36:52.0'),
  ('AU','Australia',3,'2021-06-13 12:36:52.0'),
  ('BE','Belgium',1,'2021-06-13 12:36:52.0'),
  ('BR','Brazil',2,'2021-06-13 12:36:52.0'),
  ('CA','Canada',2,'2021-06-13 12:36:52.0'),
  ('CH','Switzerland',1,'2021-06-13 12:36:52.0'),
  ('CN','China',3,'2021-06-13 12:36:52.0'),
  ('DE','Germany',1,'2021-06-13 12:36:52.0'),
  ('DK','Denmark',1,'2021-06-13 12:36:52.0'),
  ('EG','Egypt',4,'2021-06-13 12:36:52.0'),
  ('FR','France',1,'2021-06-13 12:36:52.0'),
  ('IL','Israel',4,'2021-06-13 12:36:52.0'),
  ('IN','India',3,'2021-06-13 12:36:52.0'),
  ('IT','Italy',1,'2021-06-13 12:36:52.0'),
  ('JP','Japan',3,'2021-06-13 12:36:52.0'),
  ('KW','Kuwait',4,'2021-06-13 12:36:52.0'),
  ('ML','Malaysia',3,'2021-06-13 12:36:52.0'),
  ('MX','Mexico',2,'2021-06-13 12:36:52.0'),
  ('NG','Nigeria',4,'2021-06-13 12:36:52.0'),
  ('NL','Netherlands',1,'2021-06-13 12:36:52.0'),
  ('SG','Singapore',3,'2021-06-13 12:36:52.0'),
  ('UK','United Kingdom',1,'2021-06-13 12:36:52.0'),
  ('US','United States of America',2,'2021-06-13 12:36:52.0'),
  ('ZM','Zambia',4,'2021-06-13 12:36:52.0'),
  ('ZW','Zimbabwe',4,'2021-06-13 12:36:52.0');



-- Populate Table LOCATIONS
---------------------------
INSERT INTO LOCATIONS (LOCATION_ID,STREET_ADDRESS,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY_ID) 
VALUES
  (1000,'1297 Via Cola di Rie','00989','Roma',NULL,'IT'),
  (1100,'93091 Calle della Testa','10934','Venice',NULL,'IT'),
  (1200,'2017 Shinjuku-ku','1689','Tokyo','Tokyo Prefecture','JP'),
  (1300,'9450 Kamiya-cho','6823','Hiroshima',NULL,'JP'),
  (1400,'2014 Jabberwocky Rd','26192','Southlake','Texas','US'),
  (1500,'2011 Interiors Blvd','99236','South San Francisco','California','US'),
  (1600,'2007 Zagora St','50090','South Brunswick','New Jersey','US'),
  (1700,'2004 Charade Rd','98199','Seattle','Washington','US'),
  (1800,'147 Spadina Ave','M5V 2L7','Toronto','Ontario','CA'),
  (1900,'6092 Boxwood St','YSW 9T2','Whitehorse','Yukon','CA'),
  (2000,'40-5-12 Laogianggen','190518','Beijing',NULL,'CN'),
  (2100,'1298 Vileparle (E)','490231','Bombay','Maharashtra','IN'),
  (2200,'12-98 Victoria Street','2901','Sydney','New South Wales','AU'),
  (2300,'198 Clementi North','540198','Singapore',NULL,'SG'),
  (2400,'8204 Arthur St',NULL,'London',NULL,'UK'),
  (2500,'Magdalen Centre, The Oxford Science Park','OX9 9ZB','Oxford','Oxford','UK'),
  (2600,'9702 Chester Road','09629850293','Stretford','Manchester','UK'),
  (2700,'Schwanthalerstr. 7031','80925','Munich','Bavaria','DE'),
  (2800,'Rua Frei Caneca 1360 ','01307-002','Sao Paulo','Sao Paulo','BR'),
  (2900,'20 Rue des Corps-Saints','1730','Geneva','Geneve','CH'),
  (3000,'Murtenstrasse 921','3095','Bern','BE','CH'),
  (3100,'Pieter Breughelstraat 837','3029SK','Utrecht','Utrecht','NL'),
  (3200,'Mariano Escobedo 9991','11932','Mexico City','Distrito Federal,','MX');
     


-- Populate Table JOBS
----------------------
INSERT INTO JOBS (JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY) 
VALUES
  ('AD_PRES','President',20080,40000),
  ('AD_VP','Administration Vice President',15000,30000),
  ('AD_ASST','Administration Assistant',3000,6000),
  ('FI_MGR','Finance Manager',8200,16000),
  ('FI_ACCOUNT','Accountant',4200,9000),
  ('AC_MGR','Accounting Manager',8200,16000),
  ('AC_ACCOUNT','Public Accountant',4200,9000),
  ('SA_MAN','Sales Manager',10000,20080),
  ('SA_REP','Sales Representative',6000,12008),
  ('PU_MAN','Purchasing Manager',8000,15000),
  ('PU_CLERK','Purchasing Clerk',2500,5500),
  ('ST_MAN','Stock Manager',5500,8500),
  ('ST_CLERK','Stock Clerk',2008,5000),
  ('SH_CLERK','Shipping Clerk',2500,5500),
  ('IT_PROG','Programmer',4000,10000),
  ('MK_MAN','Marketing Manager',9000,15000),
  ('MK_REP','Marketing Representative',4000,9000),
  ('HR_REP','Human Resources Representative',4000,9000),
  ('PR_REP','Public Relations Representative',4500,10500);




-- Populate Table JOBS_SNAPSHOT
-------------------------------
INSERT INTO JOBS_SNAPSHOT (JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY) 
VALUES
  ('AD_PRES','President',20080,40000),
  ('AD_VP','Administration Vice President',15000,30000),
  ('AD_ASST','Administration Assistant',3000,6000),
  ('FI_MGR','Finance Manager',8200,16000),
  ('FI_ACCOUNT','Accountant',4200,9000),
  ('AC_MGR','Accounting Manager',8200,16000),
  ('AC_ACCOUNT','Public Accountant',4200,9000),
  ('SA_MAN','Sales Manager',10000,20080),
  ('SA_REP','Sales Representative',6000,12008),
  ('PU_MAN','Purchasing Manager',8000,15000),
  ('PU_CLERK','Purchasing Clerk',2500,5500),
  ('ST_MAN','Stock Manager',5500,8500),
  ('ST_CLERK','Stock Clerk',2008,5000),
  ('SH_CLERK','Shipping Clerk',2500,5500),
  ('IT_PROG','Programmer',4000,10000),
  ('MK_MAN','Marketing Manager',9000,15000),
  ('MK_REP','Marketing Representative',4000,9000),
  ('HR_REP','Human Resources Representative',4000,9000),
  ('PR_REP','Public Relations Representative',4500,10500);




-- Populate Table DEPARTMENTS
------------------------------
INSERT INTO DEPARTMENTS (DEPARTMENT_ID,DEPARTMENT_NAME,MANAGER_ID,LOCATION_ID,URL) 
VALUES
  (10,'Administration',120,1700,'http://www.pb-sys.com'),
  (20,'Marketing',120,1800,'http://www.wintestgear.com'),
  (30,'Purchasing',120,1700,'http://www.google.com'),
  (40,'Human Resources',120,2400,'http://www.google.com'),
  (50,'Shipping',123,1500,'http://www.google.com'),
  (60,'IT',123,1400,'http://www.google.com'),
  (70,'Public Relations',120,2700,'http://www.google.com'),
  (80,'Sales',123,2500,NULL),
  (90,'Executive',123,1700,NULL),
  (100,'Finance',123,1700,NULL),
  (110,'Accounting',123,1700,NULL),
  (120,'Treasury',NULL,1700,NULL),
  (130,'Corporate Tax',NULL,1700,NULL),
  (140,'Control And Credit',NULL,1700,NULL),
  (150,'Shareholder Services',NULL,1700,NULL),
  (160,'Benefits',NULL,1700,NULL),
  (170,'Manufacturing',NULL,1700,NULL),
  (180,'Construction',NULL,1700,NULL),
  (190,'Contracting',NULL,1700,NULL),
  (200,'Operations',NULL,1700,NULL),
  (210,'IT Support',NULL,1700,NULL),
  (220,'NOC',NULL,1700,NULL),
  (230,'IT Helpdesk',NULL,1700,NULL),
  (240,'Government Sales',NULL,1700,NULL),
  (250,'Retail Sales',NULL,1700,NULL),
  (260,'Recruiting',NULL,1700,NULL),
  (270,'Payroll',NULL,1700,NULL);

   

-- Populate Table JOB_HISTORY
-----------------------------
INSERT INTO JOB_HISTORY (EMPLOYEE_ID,START_DATE,END_DATE,JOB_ID,DEPARTMENT_ID) 
VALUES
  (120,'2001-01-13 00:00:00.0','2006-07-24 00:00:00.0','IT_PROG',60),
  (123,'1997-09-21 00:00:00.0','2001-10-27 00:00:00.0','AC_ACCOUNT',110),
  (125,'2001-10-28 00:00:00.0','2005-03-15 00:00:00.0','AC_MGR',110),
  (127,'2004-02-17 00:00:00.0','2007-12-19 00:00:00.0','MK_REP',20),
  (130,'2006-03-24 00:00:00.0','2007-12-31 00:00:00.0','ST_CLERK',50),
  (132,'2007-01-01 00:00:00.0','2007-12-31 00:00:00.0','ST_CLERK',50),
  (135,'1995-09-17 00:00:00.0','2001-06-17 00:00:00.0','AD_ASST',90),
  (137,'2006-03-24 00:00:00.0','2006-12-31 00:00:00.0','SA_REP',80),
  (137,'2007-01-01 00:00:00.0','2007-12-31 00:00:00.0','SA_MAN',80),
  (139,'2002-07-01 00:00:00.0','2006-12-31 00:00:00.0','AC_ACCOUNT',90);




-- Populate Table EMPLOYEES
---------------------------
INSERT INTO EMPLOYEES (EMPLOYEE_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,HIRE_DATE,JOB_ID,SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID,SOME_DATE_FMT1,SOME_DATE_FMT2,SOME_DATE_FMT3,SOME_DATE_FMT4,FAKE_SSN,ZIP5,ZIP5OR9,ZIP9,EMAIL_ADDRESS) 
VALUES
  (120,'Matthew','Weiss','MWEISS','650.123.1234','2004-07-18 00:00:00.0','ST_MAN',8000,NULL,NULL,50,'20040718','07/18/2004','07-18-2004','2004-07-18','555-55-5555','12345','12345','12345-6789','mweiss@nowhere.com'),
  (123,'Shanta','Vollman','SVOLLMAN','650.123.4234','2005-10-10 00:00:00.0','ST_MAN',6500,NULL,120,50,'20051010','10/10/2005','10-10-2005','2005-10-10','123-45-6789','12345','12345-6789','12345-6789','svollman@nowhere.com'),
  (125,'Julia','Nayer','JNAYER','650.124.1214','2005-07-16 00:00:00.0','ST_CLERK',3200,NULL,120,50,'20050716','07/16/2005','07-16-2005','2005-07-16','555-55-5555','12345','12345','12345-6789','jnayer@nowhere.com'),
  (127,'James','Landry','JLANDRY','650.124.1334','2007-01-14 00:00:00.0','ST_CLERK',2400,NULL,120,50,'20070114','01/14/2007','01-14-2007','2007-01-14','555-55-5555','12345','12345-6789','12345-6789','jlandry@nowhere.com'),
  (130,'Mozhe','Atkinson','MATKINSO','650.124.6234','2005-10-30 00:00:00.0','ST_CLERK',2800,NULL,120,50,'20051030','10/30/2005','10-30-2005','2005-10-30','555-55-5555','12345','12345','12345-6789','matkinso@nowhere.com'),
  (132,'TJ','Olson','TJOLSON','650.124.8234','2007-04-10 00:00:00.0','ST_CLERK',2100,NULL,120,50,'20070410','04/10/2007','04-10-2007','2007-04-10','555-55-5555','12345','12345-6789','12345-6789','tjolson@nowhere.com'),
  (135,'Ki','Gee','KGEE','650.127.1734','2007-12-12 00:00:00.0','ST_CLERK',2400,NULL,123,50,'20071212','12/12/2007','12-12-2007','2007-12-12','555-55-5555','12345','12345-6789','12345-6789','kgee@nowhere.com'),
  (137,'Renske','Ladwig','RLADWIG','650.121.1234','2003-07-14 00:00:00.0','ST_CLERK',3600,NULL,123,50,'20030714','07/14/2003','07-14-2003','2003-07-14','555-55-5555','12345','12345','12345-6789','rladwig@nowhere.com'),
  (139,'John','Seo','JSEO','650.121.2019','2006-02-12 00:00:00.0','ST_CLERK',2700,NULL,123,50,'20060212','02/12/2006','02-12-2006','2006-02-12','555-55-5555','12345','12345-6789','12345-6789','jseo@nowhere.com'),
  (142,'Curtis','Davies','CDAVIES','650.121.2994','2005-01-29 00:00:00.0','ST_CLERK',3100,NULL,120,50,'20050129','01/29/2005','01-29-2005','2005-01-29','555-55-5555','12345','12345-6789','12345-6789','cdavies@nowhere.com'),
  (144,'Peter','Vargas','PVARGAS','650.121.2004','2006-07-09 00:00:00.0','ST_CLERK',2500,NULL,120,50,'20060709','07/09/2006','07-09-2006','2006-07-09',NULL,NULL,NULL,NULL,'pvargas@nowhere.com'),
  (147,'Alberto','Errazuriz','AERRAZUR','650.509.2877','2005-03-10 00:00:00.0','SA_MAN',12000,0.3,120,80,'20050310','03/10/2005','03-10-2005','2005-03-10',NULL,NULL,NULL,NULL,'aerrazur@nowhere.com'),
  (149,'Eleni','Zlotkey','EZLOTKEY','650.509.2878','2008-01-29 00:00:00.0','SA_MAN',10500,0.2,120,80,'20080129','01/29/2008','01-29-2008','2008-01-29',NULL,NULL,NULL,NULL,'ezlotkey@nowhere.com'),
  (152,'Peter','Hall','PHALL','650.509.2879','2005-08-20 00:00:00.0','SA_REP',9000,0.25,120,80,'20050820','08/20/2005','08-20-2005','2005-08-20',NULL,NULL,NULL,NULL,'phall@nowhere.com'),
  (154,'Nanette','Cambrault','NCAMBRAU','650.509.2880','2006-12-09 00:00:00.0','SA_REP',7500,0.2,120,80,'20061209','12/09/2006','12-09-2006','2006-12-09',NULL,NULL,NULL,NULL,'ncambrau@nowhere.com'),
  (157,'Patrick','Sully','PSULLY','650.509.2881','2004-03-04 00:00:00.0','SA_REP',9500,0.35,120,80,'20040304','03/04/2004','03-04-2004','2004-03-04',NULL,NULL,NULL,NULL,'psully@nowhere.com'),
  (160,'Louise','Doran','LDORAN','650.509.2882','2005-12-15 00:00:00.0','SA_REP',7500,0.3,120,80,'20051215','12/15/2005','12-15-2005','2005-12-15',NULL,NULL,NULL,NULL,'ldoran@nowhere.com'),
  (162,'Clara','Vishney','CVISHNEY','650.509.2876','2005-11-11 00:00:00.0','SA_REP',10500,0.25,120,80,'20051111','11/11/2005','11-11-2005','2005-11-11',NULL,NULL,NULL,NULL,'cvishney@nowhere.com'),
  (165,'David','Lee','DLEE','650.509.2876','2008-02-23 00:00:00.0','SA_REP',6800,0.1,120,80,'20080223','02/23/2008','02-23-2008','2008-02-23',NULL,NULL,NULL,NULL,'dlee@nowhere.com'),
  (167,'Amit','Banda','ABANDA','650.509.2876','2008-04-21 00:00:00.0','SA_REP',6200,0.1,120,80,'20080421','04/21/2008','04-21-2008','2008-04-21',NULL,NULL,NULL,NULL,'abanda@nowhere.com'),
  (170,'Tayler','Fox','TFOX','650.509.2876','2006-01-24 00:00:00.0','SA_REP',9600,0.2,120,80,'20060124','01/24/2006','01-24-2006','2006-01-24',NULL,NULL,NULL,NULL,'tfox@nowhere.com'),
  (172,'Elizabeth','Bates','EBATES','650.509.2876','2007-03-24 00:00:00.0','SA_REP',7300,0.15,120,80,'20070324','03/24/2007','03-24-2007','2007-03-24',NULL,NULL,NULL,NULL,'ebates@nowhere.com'),
  (175,'Alyssa','Hutton','AHUTTON','650.509.2876','2005-03-19 00:00:00.0','SA_REP',8800,0.25,120,80,'20050319','03/19/2005','03-19-2005','2005-03-19',NULL,NULL,NULL,NULL,'ahutton@nowhere.com'),
  (177,'Jack','Livingston','JLIVINGS','650.509.2876','2006-04-23 00:00:00.0','SA_REP',8400,0.2,120,80,'20060423','04/23/2006','04-23-2006','2006-04-23',NULL,NULL,NULL,NULL,'jlivings@nowhere.com'),
  (180,'Winston','Taylor','WTAYLOR','650.507.9876','2006-01-24 00:00:00.0','SH_CLERK',3200,NULL,120,50,'20060124','01/24/2006','01-24-2006','2006-01-24',NULL,NULL,NULL,NULL,'wtaylor@nowhere.com'),
  (183,'Girard','Geoni','GGEONI','650.507.9879','2008-02-03 00:00:00.0','SH_CLERK',2800,NULL,120,50,'20080203','02/03/2008','02-03-2008','2008-02-03',NULL,NULL,NULL,NULL,'ggeoni@nowhere.com'),
  (185,'Alexis','Bull','ABULL','650.509.2876','2005-02-20 00:00:00.0','SH_CLERK',4100,NULL,123,50,'20050220','02/20/2005','02-20-2005','2005-02-20',NULL,NULL,NULL,NULL,'abull@nowhere.com'),
  (187,'Anthony','Cabrio','ACABRIO','650.509.4876','2007-02-07 00:00:00.0','SH_CLERK',3000,NULL,123,50,'20070207','02/07/2007','02-07-2007','2007-02-07',NULL,NULL,NULL,NULL,'acabrio@nowhere.com'),
  (190,'Timothy','Gates','TGATES','650.505.3876','2006-07-11 00:00:00.0','SH_CLERK',2900,NULL,123,50,'20060711','07/11/2006','07-11-2006','2006-07-11',NULL,NULL,NULL,NULL,'tgates@nowhere.com'),
  (192,'Sarah','Bell','SBELL','650.501.1876','2004-02-04 00:00:00.0','SH_CLERK',4000,NULL,123,50,'20040204','02/04/2004','02-04-2004','2004-02-04',NULL,NULL,NULL,NULL,'sbell@nowhere.com'),
  (195,'Vance','Jones','VJONES','650.501.4876','2007-03-17 00:00:00.0','SH_CLERK',2800,NULL,123,50,'20070317','03/17/2007','03-17-2007','2007-03-17',NULL,NULL,NULL,NULL,'vjones@nowhere.com'),
  (197,'Kevin','Feeney','KFEENEY','650.507.9822','2006-05-23 00:00:00.0','SH_CLERK',3000,NULL,123,50,'20060523','05/23/2006','05-23-2006','2006-05-23',NULL,NULL,NULL,NULL,'kfeeney@nowhere.com'),
  (108,'Nancy','Greenberg','NGREENBE','515.124.4569','2002-08-17 00:00:00.0','FI_MGR',12008,NULL,123,100,'20020817','08/17/2002','08-17-2002','2002-08-17',NULL,NULL,NULL,NULL,'ngreenbe@nowhere.com'),
  (114,'Den','Raphaely','DRAPHEAL','515.127.4561','2002-12-07 00:00:00.0','PU_MAN',11000,NULL,123,30,'20021207','12/07/2002','12-07-2002','2002-12-07',NULL,NULL,NULL,NULL,'drapheal@nowhere.com'),
  (119,'Karen','Colmenares','KCOLMENA','515.127.4566','2007-08-10 00:00:00.0','PU_CLERK',2500,NULL,123,30,'20070810','08/10/2007','08-10-2007','2007-08-10',NULL,NULL,NULL,NULL,'kcolmena@nowhere.com'),
  (129,'Laura','Bissot','LBISSOT','650.124.5234','2005-08-20 00:00:00.0','ST_CLERK',3300,NULL,123,50,'20050820','08/20/2005','08-20-2005','2005-08-20',NULL,NULL,NULL,NULL,'lbissot@nowhere.com'),
  (138,'Stephen','Stiles','SSTILES','650.121.2034','2005-10-26 00:00:00.0','ST_CLERK',3200,NULL,123,50,'20051026','10/26/2005','10-26-2005','2005-10-26',NULL,NULL,NULL,NULL,'sstiles@nowhere.com'),
  (146,'Karen','Partners','KPARTNER','650.121.8009','2005-01-05 00:00:00.0','SA_MAN',13500,0.3,123,80,'20050105','01/05/2005','01-05-2005','2005-01-05',NULL,NULL,NULL,NULL,'kpartner@nowhere.com'),
  (156,'Janette','King','JKING','650.121.8009','2004-01-30 00:00:00.0','SA_REP',10000,0.35,123,80,'20040130','01/30/2004','01-30-2004','2004-01-30',NULL,NULL,NULL,NULL,'jking@nowhere.com'),
  (164,'Mattea','Marvins','MMARVINS','650.121.8009','2008-01-24 00:00:00.0','SA_REP',7200,0.1,123,80,'20080124','01/24/2008','01-24-2008','2008-01-24',NULL,NULL,NULL,NULL,'mmarvins@nowhere.com');
	 
	 
	 

-- DDL Cleanup
ALTER TABLE "JOB_HISTORY" ADD CONSTRAINT "JHIST_EMP_FK" FOREIGN KEY ("EMPLOYEE_ID") REFERENCES "EMPLOYEES" ("EMPLOYEE_ID"); 

ALTER TABLE "DEPARTMENTS" ADD CONSTRAINT "DEPT_MGR_FK" FOREIGN KEY ("MANAGER_ID")  REFERENCES "EMPLOYEES" ("EMPLOYEE_ID");



