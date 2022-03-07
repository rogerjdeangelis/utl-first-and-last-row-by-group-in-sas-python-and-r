%let pgm=utl-first-and-last-row-by-group-in-sas-python-and-r;

First and last row by group in sas python and r  (by group)

    Five solutions

          1. SAS datastep
          2. SAS SQL
          3. Base R
          4. R SQL
          5. Base Python
          6. Python SQL

ithub
https://tinyurl.com/2p8vfj3v
https://github.com/rogerjdeangelis/utl-first-and-last-row-by-group-in-sas-python-and-r

Note how I can trasition from SAS, to both R and Python with SQL

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
options sasautos=( "c:/oto" sasautos );

libname sd1 "d:/sd1";

 proc sql;
  create
     table sd1.df (
      District varchar(3)
     ,Sector   varchar(8)
     ,Name     varchar(8)
     ,Before   float
     ,After    float
     ,Age      float );
   Insert into sd1.df(District ,Sector ,Name ,Before ,After ,Age )
   values('I',   'North', 'Patton',   17,  27,  22)
   values('I',   'South', 'Joyner',   13,  22,  19)
   values('I',   'East',  'Williams', 111, 121, 29)
   values('I',   'West',  'Jurat',    51,  55,  22)
   values('II',  'North', 'Aden',     71,  70,  17)
   values('II',  'South', 'Tanner',   113, 122, 32)
   values('II',  'East',  'Jenkins',  99,  99,  24)
   values('II',  'West',  'Milner',   15,  65,  22)
   values('III', 'North', 'Chang',    69,  101, 21)
   values('III', 'South', 'Gupta',    11,  22,  21)
   values('III', 'East',  'Haskins',  45,  41,  19)
   values('III', 'West',  'LeMay',    35,  69,  20)
   values('III', 'West',  'LeMay',    35,  69,  20)
;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs SD1.DF total obs=13 07MAR2022:11:14:11                                                                    */
/*                                                                                                                        */
/* Obs    DISTRICT    SECTOR    NAME        BEFORE    AFTER    AGE                                                        */
/*                                                                                                                        */
/*   1      I         North     Patton         17       27      22                                                        */
/*   2      I         South     Joyner         13       22      19                                                        */
/*   3      I         East      Williams      111      121      29                                                        */
/*   4      I         West      Jurat          51       55      22                                                        */
/*   5      II        North     Aden           71       70      17                                                        */
/*   6      II        South     Tanner        113      122      32                                                        */
/*   7      II        East      Jenkins        99       99      24                                                        */
/*   8      II        West      Milner         15       65      22                                                        */
/*   9      III       North     Chang          69      101      21                                                        */
/*  10      III       South     Gupta          11       22      21                                                        */
/*  11      III       East      Haskins        45       41      19                                                        */
/*  12      III       West      LeMay          35       69      20                                                        */
/*  13      III       West      LeMay          35       69      20                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs WORK.SASFSTLST total obs=3 07MAR2022:11:16:51                                                             */
/*                                                                                                                        */
/* Obs    DISTRICT    SECTOR     NAME     BEFORE    AFTER    AGE                                                          */
/*                                                                                                                        */
/*  1       I         North     Patton      17        27      22                                                          */
/*  2       II        North     Aden        71        70      17                                                          */
/*  3       III       North     Chang       69       101      21                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*         _       _            _
/ |     __| | __ _| |_ __ _ ___| |_ ___ _ __
| |    / _` |/ _` | __/ _` / __| __/ _ \ `_ \
| |_  | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_(_)  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                                       |_|
*/

data sasFstLst;
  set sd1.df;
  by district;
  if first.district;
run;quit;


/*___                                _
|___ \     ___  __ _ ___   ___  __ _| |
  __) |   / __|/ _` / __| / __|/ _` | |
 / __/ _  \__ \ (_| \__ \ \__ \ (_| | |
|_____(_) |___/\__,_|___/ |___/\__, |_|
                                  |_|
*/

proc sql;

  create
      table want as
  select
      *
  from
      (select monotonic() as seq, * from sd1.df group by district)
  group
      by district
  having
      seq = min(seq)
;quit;


/*____    _
|___ /   | |__   __ _ ___  ___   _ __
  |_ \   | `_ \ / _` / __|/ _ \ | `__|
 ___) |  | |_) | (_| \__ \  __/ | |
|____(_) |_.__/ \__,_|___/\___| |_|

*/

* note I hade to use as.data.frame to change tible to dataframe;
* nor needed with R sql;

%utl_rbegin;
parmcards4;
library(SASxport);
library(collapse);
library(haven);
df<-read_sas("d:/sd1/df.sas7bdat");
df;
dfout<-as.data.frame(ffirst(df, g = df$DISTRICT))
dfout
str(dfout)
write.xport(dfout,file="d:/xpt/dfout.xpt");
;;;;
%utl_rend;

libname xpt xport "d:/xpt/dfout.xpt";
proc print data=xpt.dfout;
run;quit;

/*  _      ____              _
| || |    |  _ \   ___  __ _| |
| || |_   | |_) | / __|/ _` | |
|__   _|  |  _ <  \__ \ (_| | |
   |_|(_) |_| \_\ |___/\__, |_|
                          |_|
*/

%utl_submit_r64("
library(SASxport);
library(haven);
library(sqldf);
df<-read_sas('d:/sd1/df.sas7bdat');
df;
havSql = sqldf('
  select
      *
  from
      (select *, row_number() over (partition by district) as seq from df)
  group
      by district
  having
      seq = min(seq)
   ');
havSql;
write.xport(havSql,file='d:/xpt/havSql.xpt');
");

libname xpt xport "d:/xpt/havsql.xpt";
proc print data=xpt.havsql;
run;quit;

/*___     _                                  _   _
| ___|   | |__   __ _ ___  ___   _ __  _   _| |_| |__   ___  _ __
|___ \   | `_ \ / _` / __|/ _ \ | `_ \| | | | __| `_ \ / _ \| `_ \
 ___) |  | |_) | (_| \__ \  __/ | |_) | |_| | |_| | | | (_) | | | |
|____(_) |_.__/ \__,_|___/\___| | .__/ \__, |\__|_| |_|\___/|_| |_|
                                |_|    |___/
*/

%utl_pybegin;
parmcards4
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
from pandasql import sqldf
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
df, meta = pyreadstat.read_sas7bdat('d:/sd1/df.sas7bdat')
print(df)
dfout=df.groupby('DISTRICT').first()
print(dfout)
ds = xport.Dataset(dfout, name='dfout')
with open('d:/xpt/dfout.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname xpt xport "d:/xpt/dfout.xpt";
proc print data=xpt.dfout;
run;quit;

/*__                 _   _                             _
 / /_    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| (_) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
 \___/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/


%utl_pybegin;
parmcards4
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
from pandasql import sqldf
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
df, meta = pyreadstat.read_sas7bdat('d:/sd1/df.sas7bdat')
print(df)
havPySql = pdsql('''
  select
      *
  from
      (select *, row_number() over (partition by district) as seq from df)
  group
      by district
  having
      seq = min(seq)
   ''')
print(havPySql)
ds = xport.Dataset(havPySql, name='havPySql')
with open('d:/xpt/havPySql.xpt', 'wb') as f:
     xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname xpt xport "d:/xpt/havPySql.xpt";
proc print data=xpt.havPySql;
run;quit;

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
