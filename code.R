
library("RODBC")
library(Hmisc)
library(dplyr)
library(dbplyr)
library(data.table)
library(odbc)


library(dplyr)
library(RMSSQL)
#library(dplyr.mssql)

# conDplyr <- RSQLServer::src_sqlserver(server="local", 
#                                       type="sqlserver", user="HA", port: "1433", database = "DB_DIAG_DIABETES_ZONG") 
# 
# 
# 
# 
# RMSSQL::dbConnect("Driver={SQL Server Native Client 11.0};server=localhost; database=DB_DIAG_DIABETES_ZONG;trusted_connection=yes;")
# 
# 
# cn <- odbcDriverConnect(
#   connection="Driver={SQL Server Native Client 11.0};server=localhost")
# ?odbcDriverConnect
# 
# 
# library(DBI)
# con <- dbConnect(odbc::odbc(),
#                  driver = "SQL Server Native Client 11.0",
#                  database = "DB_DIAG_DIABETES_ZONG",
#                  uid = "postgres",
#                  pwd = "password",
#                  host = "localhost",
#                  port = 5432)
# 
# 
# # test right
# 
# my_db = DBI::dbConnect(odbc:odbc())
# my_db = DBI::dbConnect(odbc:odbc(),
#                        Drive = "SQL Server Native Client 11.0",
#   connection="Driver={SQL Server Native Client 11.0};server=localhost;
#   database=DB_DIAG_HYPERLIPEMIA_ZONG;trusted_connection=yes;")
# 
# 
# con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
# ?DBI::dbConnect
# mtcars_db <- copy_to(con, rownames_to_column(mtcars), "mtcars")
# 
# 
# 
# install.packages("rlang")
# install.packages("digest")
# install.packages("RODBC")
# install.packages("dbplyr")
# install.packages("RJDBC")
# install.packages("odbc")
# install.packages("rJava")
# install.packages("Hmisc")
# install.packages("dplyr")
# install.packages("devtools")
# install.packages("RMSSQL")
# if (!requireNamespace("BiocManager", quietly = TRUE))
#   install.packages("BiocManager")
# BiocManager::install(version = "3.10")
# install.packages("DBI")
# install.packages("RJDBC")
# install.packages("rJava")
# devtools::install_github("bescoto/dplyr.mssql") # this package
# devtools::install_github("bescoto/RMSSQL") # this package
# 
# 
# 
# library(RJDBC)
# drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/sqljdbc4.jar") 
# 
# 
# library(devtools)
# 
# 
# 
# library(rJava)
# Sys.setenv(JAVA_HOME='C:\\Program Files/Java/jre1.8.0_231/') # for 64-bit version
# library(rJava)
# 
# my_server="ABC05"
# my_db="myDatabaseName"
# my_username="JohnDoe"
# my_pwd="mVwpR55zobUldrdtXqeHez"
# 
# "ASSAY_REPORT"
# 
# df <- sqlQuery(cn, "Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from LDL_PERSON_CODE_ALL_3_1 a
# join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
# group by master_index ) b  on a.master_index=b.master_index
# where a.MASTER_INDEX in 
# (Select distinct  * From NON_TATING  )")






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

cn <- odbcDriverConnect(
  connection=
    "Driver={SQL Server Native Client 11.0};server=localhost; 
  database=DB_LPA_ZONG;
  trusted_connection=yes;")
source("R/tools.R",encoding="utf-8")

##########################################################LAP outPatients
df_lpa_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from TATING_LPA a order by MASTER_INDEX,age") )
write.csv(df_lpa_tating,file = "df_lpa_tating.csv",row.names = F) 
df_lpa_tating_trend = trendAnalysis(df_lpa_tating,1)
head(df_lpa_tating_trend)

df_lpa_non_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from NON_TATING_LPA a order by MASTER_INDEX") )
df_lpa_non_tating_trend = trendAnalysis(df_lpa_non_tating,2)
head(df_lpa_non_tating)

df_ldl_C_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from TATING_LDL_C a order by MASTER_INDEX ,age") )
df_ldl_C_tating_trend = trendAnalysis(df_ldl_C_tating,1)


df_ldl_C_non_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from NON_TATING_LDL_C a order by MASTER_INDEX ,age") )
df_ldl_C_non_tating_trend = trendAnalysis(df_ldl_C_non_tating,1)

tating_info=tbl_df(
  sqlQuery(cn, "select distinct * from  tating_into_demo order by MASTER_INDEX ") )


##################tating group
df_lpa_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from TATING_LPA a order by MASTER_INDEX,age") )
write.csv(df_lpa_tating,file = "df_lpa_tating.csv",row.names = F) 

df_lpa = df_lpa_tating
head(df_lpa)
table(df_lpa$sex)
df_lpa$sex = as.character(df_lpa$sex)

#男1女2其他


df_lpa[grepl(pattern = "男",x=df_lpa$sex),]$sex = 1
df_lpa[grepl(pattern = "女",x=df_lpa$sex),]$sex = 2
df_lpa[!df_lpa$sex%in%c(1,2),]$sex = 3



df_lpa$RESULTS=as.numeric(as.character(df_lpa$RESULTS))

min(df_lpa$RESULTS<0)

df_lpa[df_lpa$RESULTS<0,]



df_lpa = na.omit(df_lpa)
df_lpa$order =c(1:nrow(df_lpa))

head(df_lpa)

fivenum(df_lpa$RESULTS) #-1.80   33.50   78.96  208.10 4270.60
fivenum(df_lpa$age) #8  48  57  65 119

by_master_index <- group_by(df_lpa, MASTER_INDEX)
nrow(by_master_index)

models <- by_master_index %>% do(mod = lm( RESULTS~order+age, data = .))

tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))

table(tatingSign)
#tatingSign
#-1    1 
#955 2166 
#1306 1815 年龄调整后
1851/1306


basicData <- by_master_index %>% 
  summarise(mean= median(RESULTS),LAP_First=first(RESULTS),LAP_LAST= last(RESULTS),
            age=min(age),ORG_CODE=first(ORG_CODE),gender=first(sex))


fivenum(basicData$mean) #3.1000   35.4000   81.8925  212.6000 1601.1000
fivenum(basicData$age) #8  46  55  63 117
table(basicData$gender)
#1    2    3 
#1491 1617   34

#他汀数据整合

tating_res = basicData
tating_res$tating = 1
tating_res$lpa_outcome = tatingSign
head(tating_res)
nrow(tating_res)
tating_res =as.data.frame(tating_res)

##################non tating group

df_lpa_non_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from NON_TATING_LPA a order by MASTER_INDEX  --63922") )
write.csv(df_lpa_non_tating,file = "df_lpa_non_tating.csv")
df_lpa = df_lpa_non_tating
head(df_lpa)
table(df_lpa$sex)
df_lpa$sex = as.character(df_lpa$sex)

#男1女2其他


df_lpa[grepl(pattern = "男",x=df_lpa$sex),]$sex = 1
df_lpa[grepl(pattern = "女",x=df_lpa$sex),]$sex = 2
df_lpa[!df_lpa$sex%in%c(1,2),]$sex = 3



df_lpa$RESULTS=as.numeric(as.character(df_lpa$RESULTS))

min(df_lpa$RESULTS<0)

df_lpa[df_lpa$RESULTS<0,]



df_lpa = na.omit(df_lpa)
df_lpa$order =c(1:nrow(df_lpa))

head(df_lpa)

fivenum(df_lpa$RESULTS) # -43.50   35.00   83.53  202.00 5432.60
fivenum(df_lpa$age) # 1  46  55  65 119

by_master_index <- group_by(df_lpa, MASTER_INDEX)
nrow(by_master_index)

models <- by_master_index %>% do(mod = lm( RESULTS~order+age, data = .))

tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))

table(tatingSign)
#tatingSign
#-1    1 
#5722  14891 
#5240   15373 
#15373 / 5240

basicData <- by_master_index %>% 
  summarise(mean= median(RESULTS),LAP_First=first(RESULTS),LAP_LAST= last(RESULTS),
            age=min(age),ORG_CODE=first(ORG_CODE),gender=first(sex))




fivenum(basicData$mean) #3.1000   35.4000   81.8925  212.6000 1601.1000
fivenum(basicData$age) #8  46  55  63 117
table(basicData$gender)
#1    2    3 
#1491 1617   34

tmp = basicData
tmp$tating = 0
tmp$lpa_outcome = tatingSign
tmp = as.data.frame(tmp)
rownames(tmp)=c(1:nrow(tmp))+4000
head(tmp)
rownames(tating_res)



tating_res = as.matrix(tating_res)

outpatient_res = as.data.frame(rbind(as.matrix(tating_res),as.matrix(tmp)))
head(outpatient_res)


Logic_glm =glm( lpa~age+P_2+tating,family=binomial(link='logit'),data=Patient_all )




##########################################################LDL_C outPatients
############tating group
df_ldl_C_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from TATING_LDL_C a order by MASTER_INDEX ,age") )
write.csv(df_ldl_C_tating,file = "df_ldl_C_tating.csv")
df_lpa = df_ldl_C_tating
head(df_lpa)
table(df_lpa$sex)
df_lpa$sex = as.character(df_lpa$sex)

#男1女2其他


df_lpa[grepl(pattern = "男",x=df_lpa$sex),]$sex = 1
df_lpa[grepl(pattern = "女",x=df_lpa$sex),]$sex = 2
df_lpa[!df_lpa$sex%in%c(1,2),]$sex = 3



df_lpa$RESULTS=as.numeric(as.character(df_lpa$RESULTS))

min(df_lpa$RESULTS<0)

df_lpa[df_lpa$RESULTS<0,]



df_lpa = na.omit(df_lpa)
df_lpa$order =c(1:nrow(df_lpa))

head(df_lpa)

fivenum(df_lpa$RESULTS) #  0.01  2.47  3.27  4.10 23.40
fivenum(df_lpa$age) #  8  50  58  66 119

by_master_index <- group_by(df_lpa, MASTER_INDEX)
nrow(by_master_index)

models <- by_master_index %>% do(mod = lm( RESULTS~order+age, data = .))

tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))

table(tatingSign)
#tatingSign
#-1    1 
#621  1605 
#1605/621

basicData <- by_master_index %>% 
  summarise(mean= median(RESULTS),LAP_First=first(RESULTS),LAP_LAST= last(RESULTS),
            age=min(age),ORG_CODE=first(ORG_CODE),gender=first(sex))




fivenum(basicData$mean) # 0.1450  2.6475  3.3000  3.9800 11.4200
fivenum(basicData$age) # 8  49  57  64 117
table(basicData$gender)
#1    2    3 
#979 1242   27 

#他汀数据整合

tating_res = basicData
tating_res$tating = 1
tating_res$ldl_c_outcome = tatingSign
head(tating_res)
nrow(tating_res)
tating_res =as.data.frame(tating_res)

##################non tating group

df_ldl_C_non_tating=tbl_df(
  sqlQuery(cn, "Select  distinct *  from NON_TATING_LDL_C a order by MASTER_INDEX ,age") )
write.csv(df_ldl_C_non_tating,file = "df_ldl_C_non_tating.csv")
df_lpa = df_ldl_C_non_tating
head(df_lpa)
table(df_lpa$sex)
df_lpa$sex = as.character(df_lpa$sex)

#男1女2其他


df_lpa[grepl(pattern = "男",x=df_lpa$sex),]$sex = 1
df_lpa[grepl(pattern = "女",x=df_lpa$sex),]$sex = 2
df_lpa[!df_lpa$sex%in%c(1,2),]$sex = 3



df_lpa$RESULTS=as.numeric(as.character(df_lpa$RESULTS))

min(df_lpa$RESULTS<0)

df_lpa[df_lpa$RESULTS<0,]



df_lpa = na.omit(df_lpa)
df_lpa$order =c(1:nrow(df_lpa))

head(df_lpa)

fivenum(df_lpa$RESULTS) # -43.50   35.00   83.53  202.00 5432.60
fivenum(df_lpa$age) # 1  46  55  65 119

by_master_index <- group_by(df_lpa, MASTER_INDEX)
nrow(by_master_index)

models <- by_master_index %>% do(mod = lm( RESULTS~order+age, data = .))

tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))

table(tatingSign)
#tatingSign
#-1    1 
#7598  14812 

#14812 / 7598

basicData <- by_master_index %>% 
  summarise(mean= median(RESULTS),LAP_First=first(RESULTS),LAP_LAST= last(RESULTS),
            age=min(age),ORG_CODE=first(ORG_CODE),gender=first(sex))




fivenum(basicData$mean) #3.1000   35.4000   81.8925  212.6000 1601.1000
fivenum(basicData$age) #8  46  55  63 117
table(basicData$gender)
#1    2    3 
#1491 1617   34

tmp = basicData
tmp$tating = 0
tmp$ldl_C_outcome = tatingSign
tmp = as.data.frame(tmp)
rownames(tmp)=c(1:nrow(tmp))+4000
head(tmp)
rownames(tating_res)



tating_res = as.matrix(tating_res)

outpatient_ldl_c_res = as.data.frame(rbind(as.matrix(tating_res),as.matrix(tmp)))
head(outpatient_ldl_c_res)

nrow(outpatient_ldl_c_res)

Logic_glm =glm( ldl_c_outcome~as.numeric(age)+as.numeric(gender)+tating,family=binomial(link='logit'),data=outpatient_ldl_c_res )
summary(Logic_glm)







############################################################outpatients

df_lpa=tbl_df(
  sqlQuery(cn, "select distinct [master_index],RESULTS from [master].[dbo].LPA_OUT a where a.MASTER_INDEX in
           (select MASTER_INDEX from LPA_OUT  a group by MASTER_INDEX having count(DISTINCT REG_CODE)>1 )") )

tating = tbl_df(
  sqlQuery(cn, "Select distinct  * From [master].[dbo].TATING_OUT  order by master_INDEX") )

non_tating = tbl_df(
  sqlQuery(cn, "Select distinct  * From  [master].[dbo].NON_TATING_OUT  order by master_INDEX") )



df_lpa_tating = df_lpa[as.character(df_lpa$master_index)%in%as.character(tating$master_index),]

df_lpa_tating$RESULTS = as.numeric(as.character(df_lpa_tating$RESULTS))


tating_master_id = levels(as.factor(df_lpa_tating$master_index))



df_lpa_tating$order = as.numeric(rownames(df_lpa_tating))
df_lpa_tating = na.omit(df_lpa_tating )
nrow(df_lpa_tating)




by_cyl <- group_by(df_lpa_tating, master_index)
nrow(by_cyl)



models <- by_cyl %>% do(mod = lm( RESULTS~order, data = .))


tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))
table(tatingSign)

nrow(na.omit(tatingSign))

OUt_LPA_tating = cbind(models$master_index,tatingSign)
nrow(OUt_LPA_tating)
head(OUt_LPA_tating)
colnames(OUt_LPA_tating)=c("master_index","lpa")


############################tating LDL_C

LDL_C=tbl_df(
  sqlQuery(cn, "select distinct MASTER_INDEX,RESULTS,datediff(year,BIRTHDAY,DIAG_TIME) age,SEX, ORG_CODE from [DB_LPA_ZONG].[dbo].LDL_C_OUT
           ") )

LDL_C$SEX = ifelse(grepl(pattern = "男",x=LDL_C$SEX),1,0)

head(LDL_C)

LDL_C_tating = LDL_C[LDL_C$MASTER_INDEX%in%tating_master_id,] 

LDL_C_tating$RESULTS = as.numeric(as.character(LDL_C_tating$RESULTS))

by_LDL_C <- group_by(LDL_C_tating, MASTER_INDEX)

median <- by_LDL_C %>% summarise(mean= median(RESULTS),P_1=first(RESULTS),P_2= last(RESULTS),age=min(age),ORG_CODE=first(ORG_CODE),gender=first(SEX))


OUT_tating_LDL_C = as.data.frame(median)

nrow(OUT_tating_LDL_C)

head(OUT_tating_LDL_C)

OUT_tating_patients = merge(OUT_tating_LDL_C,OUt_LPA_tating,by.x = "MASTER_INDEX",by.y = "master_index")


######################non_tating
df_lpa_non_tating = df_lpa[as.character(df_lpa$master_index)%in%as.character(non_tating$master_index),]

non_tating_master_id = levels(as.factor(df_lpa_non_tating$master_index))

df_lpa_non_tating$order = as.numeric(rownames(df_lpa_non_tating))
df_lpa_non_tating$RESULTS = as.numeric(as.character(df_lpa_non_tating$RESULTS))
df_lpa_non_tating = na.omit(df_lpa_non_tating)
nrow(df_lpa_non_tating )



by_cyl <- group_by(df_lpa_non_tating, master_index)

models <- by_cyl %>% do(mod = lm( RESULTS~order, data = .))
summarise(models, rsq = summary(mod)$coefficients[1,1])
non_tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))
table(non_tatingSign)
nrow(na.omit(non_tatingSign))

?chisq.test()
tab_1 = c(table(tatingSign),table(non_tatingSign))
chisq.test(rbind(table(tatingSign),table(non_tatingSign)))
OR = tab[2]* tab[3]/(tab[1]* tab[4])

703/263/(149/53)
tab_2 = tab_1+tab

chisq.test(rbind(tab_2[c(1,2)],tab_2[c(3,4)]))
tab_2[2]* tab_2[3]/(tab_2[1]* tab_2[4])

OUt_non_LPA_tating = cbind(models$master_index,non_tatingSign)
nrow(OUt_non_LPA_tating)

head(OUt_non_LPA_tating )

colnames(OUt_non_LPA_tating ) = c("master_index","lpa")
############################NON_tating LDL_C

LDL_C=tbl_df(
  sqlQuery(cn, "select distinct MASTER_INDEX,RESULTS,datediff(year,BIRTHDAY,DIAG_TIME) age, SEX, ORG_CODE from [DB_LPA_ZONG].[dbo].LDL_C_OUT
           ") )


LDL_C$SEX = ifelse(grepl(pattern = "男",x=LDL_C$SEX),1,0)


LDL_C_non_tating = LDL_C[LDL_C$MASTER_INDEX%in%non_tating_master_id ,] 

LDL_C_non_tating$RESULTS = as.numeric(as.character(LDL_C_non_tating$RESULTS))

by_LDL_C <- group_by(LDL_C_non_tating, MASTER_INDEX)

median <- by_LDL_C %>% summarise(mean= median(RESULTS),P_1=first(RESULTS),P_2= last(RESULTS),age=min(age),ORG_CODE=first(ORG_CODE),gender=first(SEX))


OUT_non_tating_LDL_C = as.data.frame(median)

head(OUT_non_tating_LDL_C)


OUT_non_tating_patients = merge(OUT_non_tating_LDL_C,OUt_non_LPA_tating,by.x = "MASTER_INDEX",by.y = "master_index")


OUT_tating_patients$tating = 1
OUT_non_tating_patients$tating = 0

OUT_patients = rbind(OUT_tating_patients,OUT_non_tating_patients)

OUT_patients = na.omit(OUT_patients)


OUT_patients[OUT_patients$lpa == -1,]$lpa = 0

Logic_glm =glm( lpa~age+P_2+tating,family=binomial(link='logit'),data=OUT_patients )

summary(Logic_glm )


t.test(OUT_non_tating_LDL_C$P_2,OUT_tating_LDL_C$P_2)

median(OUT_non_tating_LDL_C$P_2)

sd(OUT_non_tating_LDL_C$P_2)


median(na.omit(OUT_tating_LDL_C$P_2))
sd(na.omit(OUT_tating_LDL_C$P_2))

OUT_tating_LDL_C$sign = 1
OUT_non_tating_LDL_C$sign = 2

out_Patient=rbind(OUT_tating_LDL_C,OUT_non_tating_LDL_C)

head(out_Patient)
nrow(out_Patient)

out_lpa = as.data.frame(rbind(OUt_LPA_tating,OUt_non_LPA_tating))
colnames(out_lpa) = c("master_index","lpa")
intersect(out_lpa$master_index,out_Patient$MASTER_INDEX)

table(out_Patient$sign,out_Patient$gender)
chisq.test(table(out_Patient$sign,out_Patient$gender))

t.test(age~sign,data = out_Patient)

fivenum(out_Patient[out_Patient$sign==1,]$age)

fivenum(out_Patient[out_Patient$sign==1,]$P_2)


t.test(P_2~sign,data = out_Patient)

fivenum(out_Patient[out_Patient$sign==2,]$age)


fivenum(out_Patient[out_Patient$sign==2,]$P_2)



#######################################END


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
#####################################END OUTPATIENTS


###########################################################################inpatients



df_lpa=tbl_df(
  sqlQuery(cn, "select distinct [master_index],RESULTS from  [master].[dbo].[LPA] a where a.MASTER_INDEX in
           (select MASTER_INDEX from  [master].[dbo].[LPA]  a group by MASTER_INDEX having count(DISTINCT REG_CODE)>1 )") )
head(df_lpa)
nrow(df_lpa)


tating = tbl_df(
  sqlQuery(cn, "Select distinct  * From [master].[dbo].[TATING]  order by master_INDEX") )

non_tating = tbl_df(
  sqlQuery(cn, "Select distinct  * From  [master].[dbo].[NON_TATING]  order by master_INDEX") )


df_lpa_tating = df_lpa[as.character(df_lpa$master_index)%in%as.character(tating$master_index),]
df_lpa_tating$order = as.numeric(rownames(df_lpa_tating))
nrow(df_lpa_tating)

tating_master_id = levels(as.factor(df_lpa_tating$master_index))


by_cyl <- group_by(df_lpa_tating, master_index)
nrow(by_cyl)
models <- by_cyl %>% do(mod = lm( RESULTS~order, data = .))

#mod = lm( RESULTS~order,data = by_cyl[c(1:3),] )
#summary(mod)$coefficients[2,1]


as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1]))
as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,4]))

#summarise(models, rsq = summary(mod)$coefficients[1,1])

tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))
table(tatingSign)
nrow(na.omit(tatingSign))

LPA_tating = cbind(models$master_index,tatingSign)

colnames(LPA_tating) = c("master_index","lpa")

head(LPA_tating)

############################tating LDL_C

LDL_C=tbl_df(
  sqlQuery(cn, "select distinct MASTER_INDEX,RESULTS,datediff(year,BIRTHDAY,DIAG_TIME) age,SEX, ORG_CODE from [DB_LPA_ZONG].[dbo].LDL_C
           ") )

LDL_C$SEX = ifelse(grepl(pattern = "男",x=LDL_C$SEX),1,0)

head(LDL_C)

LDL_C_tating = LDL_C[LDL_C$MASTER_INDEX%in%tating_master_id,] 

LDL_C_tating$RESULTS = as.numeric(as.character(LDL_C_tating$RESULTS))

by_LDL_C <- group_by(LDL_C_tating, MASTER_INDEX)

median <- by_LDL_C %>% summarise(mean= median(RESULTS),P_1=first(RESULTS),P_2= last(RESULTS),age=min(age),ORG_CODE=first(ORG_CODE),gender=first(SEX))


tating_LDL_C = as.data.frame(median)

nrow(tating_LDL_C)


head(tating_LDL_C)

intersect(LPA_tating$master_index,tating_LDL_C$MASTER_INDEX)

tating_patients = merge(tating_LDL_C,LPA_tating,by.x = "MASTER_INDEX",by.y = "master_index")


######################
df_lpa_non_tating = df_lpa[as.character(df_lpa$master_index)%in%as.character(non_tating$master_index),]
df_lpa_non_tating$order = as.numeric(rownames(df_lpa_non_tating))

non_tating_master_id = levels(as.factor(df_lpa_non_tating $master_index))


nrow(df_lpa_non_tating )
by_cyl <- group_by(df_lpa_non_tating, master_index)

models <- by_cyl %>% do(mod = lm( RESULTS~order, data = .))
summarise(models, rsq = summary(mod)$coefficients[2,1])
non_tatingSign = sign(as.data.frame(summarise(models, rsq = summary(mod)$coefficients[2,1])))
table(non_tatingSign)
nrow(na.omit(non_tatingSign))

non_LPA_tating = cbind(models$master_index,non_tatingSign)

colnames(non_LPA_tating) = c("master_index","lpa")

head(non_LPA_tating) 

?chisq.test()

chisq.test(c(table(tatingSign),table(non_tatingSign)))
tab=c(table(tatingSign),table(non_tatingSign))
chisq.test(rbind(table(tatingSign),table(non_tatingSign)))
OR = tab[2]* tab[3]/(tab[1]* tab[4])


by_cyl <- group_by(mtcars, cyl)

models <- by_cyl %>% do(mod = lm(mpg ~ disp, data = .))
summarise(models, rsq = summary(mod)$coefficients[2,1])

summarise(models, rsq = summary(mod))

a = summary(lm(mpg ~ disp, data = by_cyl))
a$coefficients[2,1]

############################NON_tating LDL_C

LDL_C=tbl_df(
  sqlQuery(cn, "select distinct MASTER_INDEX,RESULTS,datediff(year,BIRTHDAY,DIAG_TIME) age,SEX, ORG_CODE from [DB_LPA_ZONG].[dbo].LDL_C_OUT
           ") )

LDL_C$SEX = ifelse(grepl(pattern = "男",x=LDL_C$SEX),1,0)

LDL_C_non_tating = LDL_C[LDL_C$MASTER_INDEX%in%non_tating_master_id ,] 

LDL_C_non_tating$RESULTS = as.numeric(as.character(LDL_C_non_tating$RESULTS))

by_LDL_C <- group_by(LDL_C_non_tating, MASTER_INDEX)

median <- by_LDL_C %>% summarise(mean= median(RESULTS),P_1=first(RESULTS),P_2= last(RESULTS),age=min(age),ORG_CODE=first(ORG_CODE),gender=first(SEX))


non_tating_LDL_C = as.data.frame(median)


non_tating_patients = merge(non_tating_LDL_C,non_LPA_tating,by.x = "MASTER_INDEX",by.y = "master_index")


tating_patients$tating=1


non_tating_patients$tating=0


Patient = rbind(tating_patients,non_tating_patients)

Patient = na.omit(Patient)

Patient[Patient$lpa==-1,]$lpa = 0

Patient_all = rbind(OUT_patients,Patient)

Logic_glm =glm( lpa~age+P_2+tating,family=binomial(link='logit'),data=Patient_all )

summary(Logic_glm )


nrow(Patient_all)



t.test(non_tating_LDL_C$P_2,tating_LDL_C$P_2)





tating_LDL_C$sign = 1
non_tating_LDL_C$sign = 2

Patient=rbind(tating_LDL_C,non_tating_LDL_C)


table(Patient$sign,Patient$gender)
chisq.test(table(Patient$sign,Patient$gender))

t.test(age~sign,data = Patient)

fivenum(Patient[Patient$sign==1,]$age)

fivenum(Patient[Patient$sign==1,]$P_2)


t.test(P_2~sign,data = Patient)

fivenum(Patient[Patient$sign==2,]$age)


fivenum(Patient[Patient$sign==2,]$P_2)





OUt_LPA_tating
OUt_non_LPA_tating 
out_Patient
LPA_tating
non_LPA_tating
Patient

?merge
out_Patient_1 = merge(out_Patient,as.data.frame(rbind(OUt_LPA_tating,OUt_non_LPA_tating)),
                      by.x= "MASTER_INDEX",by.y= "models$master_index")

out_Patient_2 = merge(Patient,as.data.frame(rbind(LPA_tating,non_LPA_tating)),
                      by.x= "MASTER_INDEX",by.y= "models$master_index")

