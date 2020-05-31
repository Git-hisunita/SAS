LIBNAME SRC '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb';


data WORK.IMPORT;
  infile '//opr.statefarm.org/dfs/corp/00/workgroup/unit-uc/Data Mart/ETL/MissingTrades/pamsb/01MISS.txt' firstobs=1 truncover;
  input
    INTN_SECUR_ID $ 1-15
    SYS_SRC_CD $ 16-19
    LOT_NUM $ 21-30
    PFOLIO_CD $ 31-34
    TRD_NUM $ 35-44
    TRANS_NUM $ 45-51  ;
 run;

PROC SQL;
	CREATE TABLE MISS_GRP AS 
		SELECT t1.INTN_SECUR_ID, 
			t1.SYS_SRC_CD, 
			t1.LOT_NUM, 
			t1.PFOLIO_CD, 
			t1.TRD_NUM, 
			t1.TRANS_NUM
		FROM WORK.IMPORT t1
			GROUP BY 
			t1.INTN_SECUR_ID, 
			t1.SYS_SRC_CD, 
			t1.LOT_NUM, 
			t1.PFOLIO_CD;
QUIT;

PROC SQL;
	CREATE TABLE MISS_GRP_SRT AS 
		SELECT * FROM MISS_GRP
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data fisrt_rec;
	set MISS_GRP_SRT;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE FIRST_FILE_SRT AS 
 SELECT * FROM fisrt_rec
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- MISS 02 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE REM_REC AS 
 SELECT * FROM MISS_GRP_SRT
 except
 SELECT * FROM FIRST_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT2 AS 
		SELECT * FROM REM_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data sec_rec;
	set MISS_GRP_SRT2;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE SEC_FILE_SRT AS 
 SELECT * FROM sec_rec
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 02 FILE -----------------------------------*/


/* ---------------------------- MISS 03 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE THIRD_FILE_REC AS 
 SELECT * FROM REM_REC
 except
 SELECT * FROM SEC_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT3 AS 
		SELECT * FROM THIRD_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data THIRD_REC;
	set MISS_GRP_SRT3;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE THIRD_FILE_SRT AS 
 SELECT * FROM THIRD_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 03 FILE -----------------------------------*/


 /* ---------------------------- MISS 04 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE FRTH_FILE_REC AS 
 SELECT * FROM THIRD_FILE_REC
 except
 SELECT * FROM THIRD_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT4 AS 
		SELECT * FROM FRTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data FRTH_REC;
	set MISS_GRP_SRT4;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE FRTH_FILE_SRT AS 
 SELECT * FROM FRTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 04 FILE -----------------------------------*/



 /* ---------------------------- MISS 05 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE FIFTH_FILE_REC AS 
 SELECT * FROM FRTH_FILE_REC
 except
 SELECT * FROM FRTH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT5 AS 
		SELECT * FROM FIFTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data FIFTH_REC;
	set MISS_GRP_SRT5;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE FIFTH_FILE_SRT AS 
 SELECT * FROM FIFTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;


/* ---------------------------- END OF MISS 05 FILE -----------------------------------*/
 
 /* ---------------------------- MISS 06 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE SIXTH_FILE_REC AS 
 SELECT * FROM FIFTH_FILE_REC
 except
 SELECT * FROM FIFTH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT6 AS 
		SELECT * FROM SIXTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data SIXTH_REC;
	set MISS_GRP_SRT6;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE SIXTH_FILE_SRT AS 
 SELECT * FROM SIXTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 06 FILE -----------------------------------*/

/* ---------------------------- MISS 07 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE SVNTH_FILE_REC AS 
 SELECT * FROM SIXTH_FILE_REC
 except
 SELECT * FROM SIXTH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT7 AS 
		SELECT * FROM SVNTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data SVNTH_REC;
	set MISS_GRP_SRT7;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE SVNTH_FILE_SRT AS 
 SELECT * FROM SVNTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 07 FILE -----------------------------------*/

/* ---------------------------- MISS 08 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE EGTH_FILE_REC AS 
 SELECT * FROM SVNTH_FILE_REC
 except
 SELECT * FROM SVNTH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT8 AS 
		SELECT * FROM EGTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data EGTH_REC;
	set MISS_GRP_SRT8;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;
	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE EGTH_FILE_SRT AS 
 SELECT * FROM EGTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 08 FILE -----------------------------------*/

/* ---------------------------- MISS 09 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE NITH_FILE_REC AS 
 SELECT * FROM EGTH_FILE_REC
 except
 SELECT * FROM EGTH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT9 AS 
		SELECT * FROM NITH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data NITH_REC;
	set MISS_GRP_SRT9;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE NITH_FILE_SRT AS 
 SELECT * FROM NITH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 09 FILE -----------------------------------*/

/* ---------------------------- MISS 10 FILE -----------------------------------*/
PROC SQL;
CREATE TABLE TNTH_FILE_REC AS 
 SELECT * FROM NITH_FILE_REC
 except
 SELECT * FROM NITH_FILE_SRT;
 RUN;

PROC SQL;
 	CREATE TABLE MISS_GRP_SRT10 AS 
		SELECT * FROM TNTH_FILE_REC
			ORDER BY  INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD;
RUN;

data TNTH_REC;
	set MISS_GRP_SRT10;
	by INTN_SECUR_ID SYS_SRC_CD LOT_NUM PFOLIO_CD ;

	if first.INTN_SECUR_ID or first.sys_src_cd or first.lot_num or first.pfolio_cd  then
		output;
	else delete;
run;

PROC SQL;
 CREATE TABLE TNTH_FILE_SRT AS 
 SELECT * FROM TNTH_REC
 ORDER BY INTN_SECUR_ID, SYS_SRC_CD, LOT_NUM, PFOLIO_CD, TRD_NUM, TRANS_NUM;
 RUN;
/* ---------------------------- END OF MISS 10 FILE -----------------------------------*/

/*libname out 'C:\INVEST\Missing Trades\MissingTradesTest'; */


/********************* Output Files *************************************/

data _null_;
   set FIRST_FILE_SRT Nobs=nobs;

      file First_Fl;
	  filename First_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS1.txt';

   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

data _null_;
 set SEC_FILE_SRT ;
   file Sec_Fl;
   filename Sec_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS2.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

data _null_;
 set THIRD_FILE_SRT Nobs=nobs;
    file Trd_Fl;
	filename Trd_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS3.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;


data _null_;
 set FRTH_FILE_SRT ;
    file Frth_Fl;
	filename Frth_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS4.txt';

   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;


data _null_;
 set FIFTH_FILE_SRT;
   file Fifth_Fl;
	filename Fifth_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS5.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;
/************ Sixth File *****************/

data _null_;
 set SIXTH_FILE_SRT;
   file SIXTH_Fl;
	filename SIXTH_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS6.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

/************ Seventh File *****************/

data _null_;
 set SVNTH_FILE_SRT;
   file SVNTH_Fl;
	filename SVNTH_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS7.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

/************ Eighth File *****************/

data _null_;
 set EGTH_FILE_SRT;
   file EGTH_Fl;
	filename EGTH_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS8.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

/************ Ninth File *****************/

data _null_;
 set NITH_FILE_SRT;
   file NITH_Fl;
	filename NITH_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS9.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

/************ Tenth File *****************/

data _null_;
 set TNTH_FILE_SRT;
   file TNTN_Fl;
	filename TNTN_Fl '\\opr.statefarm.org\dfs\corp\00\workgroup\unit-uc\Data Mart\ETL\MissingTrades\pamsb\output\01MISS10.txt';
   put
          INTN_SECUR_ID 0 -15
          SYS_SRC_CD 16-18
          LOT_NUM 21-30
          PFOLIO_CD 31-34
          TRD_NUM 36-45
          TRANS_NUM 46-52  ;
run;

/*****************************/