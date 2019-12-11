
library("RODBC")
library(Hmisc)
library(dplyr)
library(dbplyr)
library(data.table)
library(odbc)


library(dplyr)
library(RMSSQL)
#library(dplyr.mssql)

conDplyr <- RSQLServer::src_sqlserver(server="local", 
                                      type="sqlserver", user="HA", port: "1433", database = "DB_DIAG_DIABETES_ZONG") 




RMSSQL::dbConnect("Driver={SQL Server Native Client 11.0};server=localhost; database=DB_DIAG_DIABETES_ZONG;trusted_connection=yes;")


cn <- odbcDriverConnect(
  connection="Driver={SQL Server Native Client 11.0};server=localhost")
?odbcDriverConnect


library(DBI)
con <- dbConnect(odbc::odbc(),
                 driver = "SQL Server Native Client 11.0",
                 database = "DB_DIAG_DIABETES_ZONG",
                 uid = "postgres",
                 pwd = "password",
                 host = "localhost",
                 port = 5432)


# test right
cn <- odbcDriverConnect(
  connection="Driver={SQL Server Native Client 11.0};server=localhost; database=DB_DIAG_HYPERLIPEMIA_ZONG;trusted_connection=yes;")

my_db = DBI::dbConnect(odbc:odbc())
my_db = DBI::dbConnect(odbc:odbc(),
                       Drive = "SQL Server Native Client 11.0",
  connection="Driver={SQL Server Native Client 11.0};server=localhost;
  database=DB_DIAG_HYPERLIPEMIA_ZONG;trusted_connection=yes;")


con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
?DBI::dbConnect
mtcars_db <- copy_to(con, rownames_to_column(mtcars), "mtcars")



install.packages("rlang")
install.packages("digest")
install.packages("RODBC")
install.packages("dbplyr")
install.packages("RJDBC")
install.packages("odbc")
install.packages("rJava")
install.packages("Hmisc")
install.packages("dplyr")
install.packages("devtools")
install.packages("RMSSQL")
if (!requireNamespace("BiocManager", quietly = TRUE))
  install.packages("BiocManager")
BiocManager::install(version = "3.10")
install.packages("DBI")
install.packages("RJDBC")
install.packages("rJava")
devtools::install_github("bescoto/dplyr.mssql") # this package
devtools::install_github("bescoto/RMSSQL") # this package



library(RJDBC)
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/sqljdbc4.jar") 


library(devtools)



library(rJava)
Sys.setenv(JAVA_HOME='C:\\Program Files/Java/jre1.8.0_231/') # for 64-bit version
library(rJava)

my_server="ABC05"
my_db="myDatabaseName"
my_username="JohnDoe"
my_pwd="mVwpR55zobUldrdtXqeHez"

"ASSAY_REPORT"

df <- sqlQuery(cn, "Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from LDL_PERSON_CODE_ALL_3_1 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index
where a.MASTER_INDEX in 
(Select distinct  * From NON_TATING  )")

df = tbl_df(sqlQuery(cn, "SELECT top 100 [REPORT_ID]
      ,[REPORT_CODE]
      ,[REG_ID]
      ,[REG_CODE]
      ,[EMP_CODE]
      ,[EMP_NAME]
      ,[REPORT_TIME]
      ,[SPECIMEN_TYPE]
      ,[ASSAY_ITEM_CODE]
      ,[ASSAY_ITEM_NAME]
      ,[ITEM_ENAME]
      ,[UNIT]
      ,[RESULTS]
      ,[REFRANGE]
      ,[RESULTSTATUS]
      ,[REG_SOURCE]
      ,[ORG_CODE]
      ,[UPLOAD_TIME]
  FROM [DB_DIAG_DIABETES_ZONG].[dbo].[ASSAY_REPORT]")) 
  

df = tbl_df(sqlQuery(cn, "SELECT top 100 [REPORT_ID]
      ,[REPORT_CODE]
      ,[REG_ID]
      ,[REG_CODE]
      ,[EMP_CODE]
      ,[EMP_NAME]
      ,[REPORT_TIME]
      ,[SPECIMEN_TYPE]
      ,[ASSAY_ITEM_CODE]
      ,[ASSAY_ITEM_NAME]
      ,[ITEM_ENAME]
      ,[UNIT]
      ,[RESULTS]
      ,[REFRANGE]
      ,[RESULTSTATUS]
      ,[REG_SOURCE]
      ,[ORG_CODE]
      ,[UPLOAD_TIME]
  FROM [ASSAY_REPORT]")) 

  
head(df)

? DBI::dbConnect


demostantin = read.csv("R/nontatingDemo1_5year.csv",sep = '\t')

head(demostantin)

colnames(demostantin) = c("ID","gender","age","location")

age_s = ifelse(as.numeric(as.character(demostantin$age))<=45,1,ifelse(as.numeric(as.character(demostantin$age))<70,2,3))

sum(round(table(age_s)/sum(table(age_s))*1072))

round(table(age_s))

table(demostantin$gender)

table(demostantin$location)



demostantin = read.csv("R/tatingDemo1_5year.csv",sep = '\t')

head(demostantin)

nrow(demostantin)

colnames(demostantin) = c("ID","gender","age","location")

age_s = ifelse(as.numeric(as.character(demostantin$age))<=45,1,ifelse(as.numeric(as.character(demostantin$age))<70,2,3))

sum(round(table(age_s)/sum(table(age_s))*8054))

round(table(age_s))

table(demostantin$gender)

table(demostantin$location)

