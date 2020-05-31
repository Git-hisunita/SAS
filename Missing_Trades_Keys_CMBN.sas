/*'LIBNAME SRC 'C:\INVEST\Missing Trades\InputFiles';'*/

LIBNAME TGT 'C:\INVEST\Missing Trades\InputFiles\TgtFiles';
LIBNAME MKF 'C:\INVEST\Missing Trades\InputFiles\TgtFiles';

libname invst db2 dsn=INVSTDME user='invstetl' password="{SAS002}9DBE56511ADB145B1FE33F883291FB1F"  schema='RU99';
/*libname invst db2 dsn=INVSTQRY_EDD_DEV user='invstetl' password="{SAS002}9DBE56511ADB145B1FE33F883291FB1F"  schema='RU99';*/
options obs =  MAX;

/****************** ISSUE TRANS DETL REVERSAL FILES CODE - STOCKS & BONDS **********************/
data TGT.ISTD_REV_SB    ;
      infile 'X:\bkp\20200528\01ISSUE_TRANS_DETL_REVERSALS.CSV' delimiter = ',' MISSOVER DSD  lrecl=32767 firstobs=2 ;
        informat INTN_SECUR_ID $12. ;
        informat SYS_SRC_CD $4. ;
        informat LOT_NUM best32. ;
        informat PFOLIO_CD $6. ;
        informat TRD_NUM best32. ;
        informat TRANS_NUM best32. ;
        format INTN_SECUR_ID $11. ;
        format SYS_SRC_CD $4. ;
        format LOT_NUM best12. ;
        format PFOLIO_CD $6. ;
        format TRD_NUM best12. ;
        format TRANS_NUM best12. ;
     input
                 INTN_SECUR_ID $
                 SYS_SRC_CD $
                 LOT_NUM
                 PFOLIO_CD $
                 TRD_NUM
                 TRANS_NUM     ;
      run;

data TGT.ISTD_REV_MF    ;
      infile 'Y:\bkp\20200528\02ISSUE_TRANS_DETL_REVERSALS.CSV' delimiter = ',' MISSOVER DSD  lrecl=32767 firstobs=2 ;
        informat INTN_SECUR_ID $12. ;
        informat SYS_SRC_CD $4. ;
        informat LOT_NUM best32. ;
        informat PFOLIO_CD $6. ;
        informat TRD_NUM best32. ;
        informat TRANS_NUM best32. ;
        format INTN_SECUR_ID $11. ;
        format SYS_SRC_CD $4. ;
        format LOT_NUM best12. ;
        format PFOLIO_CD $6. ;
        format TRD_NUM best12. ;
        format TRANS_NUM best12. ;
     input
                 INTN_SECUR_ID $
                 SYS_SRC_CD $
                 LOT_NUM
                 PFOLIO_CD $
                 TRD_NUM
                 TRANS_NUM     ;
      run;
data TGT.ISTD_REV_BNK    ;
      infile 'Z:\bkp\20200528\03ISSUE_TRANS_DETL_REVERSALS.CSV' delimiter = ',' MISSOVER DSD  lrecl=32767 firstobs=2 ;
        informat INTN_SECUR_ID $12. ;
        informat SYS_SRC_CD $4. ;
        informat LOT_NUM best32. ;
        informat PFOLIO_CD $6. ;
        informat TRD_NUM best32. ;
        informat TRANS_NUM best32. ;
        format INTN_SECUR_ID $11. ;
        format SYS_SRC_CD $4. ;
        format LOT_NUM best12. ;
        format PFOLIO_CD $6. ;
        format TRD_NUM best12. ;
        format TRANS_NUM best12. ;
     input
                 INTN_SECUR_ID $
                 SYS_SRC_CD $
                 LOT_NUM
                 PFOLIO_CD $
                 TRD_NUM
                 TRANS_NUM     ;
      run;
PROC SQL;
 Create table TGT.ISTD_REV_TOT
 AS  SELECT * FROM TGT.ISTD_REV_SB
 UNION
     SELECT * FROM TGT.ISTD_REV_MF
 UNION 
     SELECT * FROM TGT.ISTD_REV_BNK;
    run;
PROC SQL;
	create table ISTD_REV_SRT AS 
		SELECT * FROM TGT.ISTD_REV_TOT t1
		ORDER BY t1.INTN_SECUR_ID,
               t1.SYS_SRC_CD,
               t1.LOT_NUM,
               t1.PFOLIO_CD,
               t1.TRD_NUM,
               t1.TRANS_NUM;
QUIT;

PROC SQL;
CREATE TABLE ISTD_REV_SEC AS 
SELECT DISTINCT INTN_SECUR_ID FROM ISTD_REV_SRT
ORDER BY INTN_SECUR_ID;
QUIT;

PROC SQL;
CREATE TABLE ISTS_SEC AS 
 SELECT distinct A.INTN_SECUR_ID FROM ISTD_REV_SEC  A 
 LEFT JOIN INVST.ISSUE_TRANS_SUMRY B 
ON A.INTN_SECUR_ID = B.INTN_SECUR_ID ;
 QUIT;

PROC SQL;
CREATE TABLE ISTD_REV_FILE AS 
SELECT * FROM ISTD_REV_SRT WHERE INTN_SECUR_ID IN (SELECT INTN_SECUR_ID FROM ISTS_SEC) 
;
QUIT;

PROC SQL;
	CREATE TABLE ISTD_ISDTR_JN
		AS 
			SELECT X.INTN_SECUR_ID, X.SYS_SRC_CD, X.LOT_NUM, X.PFOLIO_CD, X.TRD_NUM, X.TRANS_NUM
				FROM ISTD_REV_FILE X LEFT JOIN invst.ISSUE_TRANS_DETL Y ON
					(X.TRANS_NUM = Y.TRANS_NUM)
					AND (X.TRD_NUM = Y.TRD_NUM)
					AND (X.PFOLIO_CD = Y.PAM_PFOLIO_CD)
					AND (X.LOT_NUM = Y.LOT_NUM)
					AND (X.SYS_SRC_CD = Y.SYS_SRC_CD)
					AND  (X.INTN_SECUR_ID = Y.INTN_SECUR_ID)
				WHERE (Y.TRANS_NUM) IS NULL;
/*AND X.SYS_SRC_CD = '01';*/
QUIT;

DATA MISS_FILE;
SET ISTD_ISDTR_JN;
char_LOT_NUM = put(LOT_NUM, $8.);
char_TRD_NUM = put(TRD_NUM, $8.);
char_TRANS_NUM = put(TRANS_NUM, $8.);
drop LOT_NUM TRD_NUM TRANS_NUM;
rename char_LOT_NUM = LOT_NUM;
rename char_TRD_NUM = TRD_NUM;
rename char_TRANS_NUM = TRANS_NUM;
RUN;

DATA MISS_FILE_TRM;
SET MISS_FILE;
LOT_NUM = trim(left(LOT_NUM));
TRD_NUM = trim(left(TRD_NUM));
TRANS_NUM = trim(left(TRANS_NUM));
RUN;

PROC SQL;
 CREATE TABLE MISS_FILE_01 AS 
  SELECT t1.* FROM MISS_FILE_TRM t1
  WHERE t1.SYS_SRC_CD = '01'
ORDER BY t1.INTN_SECUR_ID,
         t1.SYS_SRC_CD,
         t1.LOT_NUM,
         t1.PFOLIO_CD,
         t1.TRD_NUM,
         t1.TRANS_NUM;
QUIT;
PROC SQL;
 CREATE TABLE MISS_FILE_02 AS 
  SELECT t1.* FROM MISS_FILE_TRM t1
  WHERE t1.SYS_SRC_CD = '02'
ORDER BY t1.INTN_SECUR_ID,
         t1.SYS_SRC_CD,
         t1.LOT_NUM,
         t1.PFOLIO_CD,
         t1.TRD_NUM,
         t1.TRANS_NUM;
QUIT;
PROC SQL;
 CREATE TABLE MISS_FILE_03 AS 
  SELECT t1.* FROM MISS_FILE_TRM t1
  WHERE t1.SYS_SRC_CD = '03'
ORDER BY t1.INTN_SECUR_ID,
         t1.SYS_SRC_CD,
         t1.LOT_NUM,
         t1.PFOLIO_CD,
         t1.TRD_NUM,
         t1.TRANS_NUM;
QUIT;

data _null_;
   set MISS_FILE_01;
file MKF;
filename MKF 'C:\INVEST\Missing Trades\InputFiles\TgtFiles\MissingKeyFiles\01MISS.txt';
put
 	INTN_SECUR_ID 0-15
    SYS_SRC_CD 16-18
    LOT_NUM 21-30
    PFOLIO_CD 31-34
    TRD_NUM 36-45
    TRANS_NUM 46-52 ;
run;

data _null_;
   set MISS_FILE_02;
file MKF;
filename MKF 'C:\INVEST\Missing Trades\InputFiles\TgtFiles\MissingKeyFiles\02MISS.txt';
put
 	INTN_SECUR_ID 0-15
    SYS_SRC_CD 16-18
    LOT_NUM 21-30
    PFOLIO_CD 31-34
    TRD_NUM 36-45
    TRANS_NUM 46-52 ;
run;

data _null_;
   set MISS_FILE_03;
file MKF;
filename MKF 'C:\INVEST\Missing Trades\InputFiles\TgtFiles\MissingKeyFiles\03MISS.txt';
put
 	INTN_SECUR_ID 0-15
    SYS_SRC_CD 16-18
    LOT_NUM 21-30
    PFOLIO_CD 31-34
    TRD_NUM 36-45
    TRANS_NUM 46-52 ;
run;

/****************** ISSUE TRANS DETL REVERSAL FILES CODE - Mutual Funds  **********************/
