 -----
  SELECT count(DISTINCT MASTER_INDEX ) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT]
  ----347588 -- 进入目录的患者人数 
  SELECT count(*) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG] where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
  ----186017 诊疗条目
  SELECT count(*) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_DIAG] where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
  ----503179 诊疗条目

  -------------------------------------------------------门诊数据
  ------------------------------------------------------------------
    --筛选/建立住院患者诊断信息
	SELECT [REG_CODE]
      ,[ICD]
      ,[DISEASE_NAME]
      ,[DIAG_TIME]
      ,[ORG_CODE]
	  INTO LDL_REG_CODE_OUT
	  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_DIAG]
  SELECT top 100 * FROM  LDL_REG_CODE 


------筛选/建立患者个人信息
SELECT TOP 100 * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_REG_INFO]
SELECT [REG_ID]
      ,[REG_CODE]
      ,[PERSON_CODE]
	  ,[CARD_ID]
	  ,F_DIAG
	  ,I_CARD_TYPE
  INTO ALL_REG_CODE_OUT -- 313044
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_REG_INFO]
SELECT TOP 100 * FROM ALL_REG_CODE

-------合并个人信息 
--DROP TABLE LDL_PERSON_CODE

SELECT LDL_REG_CODE_OUT.*
	,ALL_REG_CODE_OUT.[REG_ID]
	,ALL_REG_CODE_OUT.[PERSON_CODE]
	,ALL_REG_CODE_OUT.[CARD_ID]
	,ALL_REG_CODE_OUT.F_DIAG
	,ALL_REG_CODE_OUT.I_CARD_TYPE
  INTO LDL_PERSON_CODEL_OUT
  FROM LDL_REG_CODE_OUT  INNER JOIN ALL_REG_CODE_OUT 
  ON LDL_REG_CODE_OUT.REG_CODE=ALL_REG_CODE_OUT.REG_CODE

SELECT TOP 100 * FROM LDL_PERSON_CODEL_OUT ORDER BY PERSON_CODE, DIAG_TIME
select distinct * into  LDL_PERSON_CODE_OUT_1 from  LDL_PERSON_CODEL_OUT

  SELECT LDL_PERSON_CODE_OUT_1.*
		,[PUB_PATIENT].BIRTHDAY
		,[PUB_PATIENT].SEX
		,[PUB_PATIENT].MASTER_INDEX
INTO LDL_PERSON_CODE_ALL_OUT
FROM LDL_PERSON_CODE_OUT_1 INNER  JOIN [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT]
ON [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT].PERSON_CODE =  LDL_PERSON_CODE_OUT_1.PERSON_CODE
ORDER BY MASTER_INDEX,DIAG_TIME ASC
SELECT TOP 100 * FROM LDL_PERSON_CODE_ALL_OUT
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_OUT a
---- LDL_PERSON_CODE_ALL 人口学信息表


-----首诊"血脂异常"后的诊疗记录
drop table LDL_PERSON_CODE_ALL_FirstDiag_OUT
select a.* 
into LDL_PERSON_CODE_ALL_FirstDiag_OUT
from    LDL_PERSON_CODE_ALL_OUT  a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_OUT where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 

select top 10000 a.* from   LDL_PERSON_CODE_ALL_FirstDiag_OUT a 
order by   a.master_index,diag_TIME 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_FirstDiag_OUT a

SELECT *  into  LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT --过滤 住院次数大于2次
FROM LDL_PERSON_CODE_ALL_FirstDiag_OUT where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   LDL_PERSON_CODE_ALL_FirstDiag_OUT   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>1)

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT a 
--100465


------------------他汀组
drop table  TATING_OUT
select a.master_index 
into TATING_OUT
from (select distinct master_index from   LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT) a
 join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from    LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[OUT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX 

Select distinct  * From TATING_OUT  order by master_INDEX   --59817

--非他汀组
drop table NON_TATING_OUT 
select a.master_index 
into NON_TATING_OUT
from (select distinct master_index from  LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT) a
left join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from   LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[OUT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX
where b.master_INDEX is null 

Select distinct  * From NON_TATING_OUT  order by master_INDEX 
--40648


-------指标检查数据
drop table LDL_LAB_OUT
SELECT 
      [REG_CODE]
      ,[REPORT_TIME]
      ,[ASSAY_ITEM_CODE]
      ,[ASSAY_ITEM_NAME]
      ,[ITEM_ENAME]
      ,[UNIT]
      ,[RESULTS]
      ,[REFRANGE],I_ITEM_ENAME
  into LDL_LAB_OUT
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[ASSAY_REPORT] where 
 ( I_ITEM_ENAME like '%APO-A%' or
  I_ITEM_ENAME like '%APO-B%' or 
  I_ITEM_ENAME like '%CRP%' or
  I_ITEM_ENAME like '%HDL' or
  I_ITEM_ENAME like '%HDL-C%' or 
  I_ITEM_ENAME like '%LDL%' or
  I_ITEM_ENAME like '%LDL-C%' or
  I_ITEM_ENAME like '%LPA%' or
  I_ITEM_ENAME like '%TC%' or
  I_ITEM_ENAME like '%TG%' or
  I_ITEM_ENAME like '%VLDL%' ) and REG_SOURCE=1

drop table LPA_OUT
Select distinct a.*,
b.REPORT_TIME,
b.ITEM_ENAME,
b.RESULTS,
b.REFRANGE 
into LPA_OUT
from LDL_PERSON_CODE_ALL_FirstDiag_2times_OUT a
join (SELECT * from   LDL_LAB_OUT a where a.I_ITEM_ENAME like '%LPA%'   ) b  on a.REG_CODE=b.REG_CODE

Select * from LPA_OUT order by MASTER_INDEX,DIAG_TIME 

select distinct * from LPA_OUT a where a.MASTER_INDEX in
           (select MASTER_INDEX from LPA_OUT  a group by MASTER_INDEX having count(DISTINCT REG_CODE)>1 )
		    order by MASTER_INDEX,DIAG_TIME 






  -------------------------------------------------------门诊数据END
  ------------------------------------------------------------------



  -------------------------------------------------------住院数据
  ------------------------------------------------------------------
  


 --筛选/建立住院患者诊断信息
  SELECT top 100 * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG]
  SELECT [REG_CODE]
      ,[ICD]
      ,[DISEASE_NAME]
      ,[DIAG_TIME]
      ,[ORG_CODE]
	  INTO LDL_REG_CODE
	  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG]
  SELECT top 100 * FROM  LDL_REG_CODE

------筛选/建立患者个人信息
SELECT TOP 100 * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_REG_INFO]
SELECT [REG_ID]
      ,[REG_CODE]
      ,[PERSON_CODE]
	  ,[CARD_ID]
	  ,OUTHOS_STATUS
	  ,F_DIAG
	  ,I_CARD_TYPE
  INTO ALL_REG_CODE -- 313044
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_REG_INFO]
SELECT TOP 100 * FROM ALL_REG_CODE

-------合并个人信息 
--DROP TABLE LDL_PERSON_CODE

SELECT LDL_REG_CODE.*
	,ALL_REG_CODE.[REG_ID]
	,ALL_REG_CODE.[PERSON_CODE]
	,ALL_REG_CODE.[CARD_ID]
	,ALL_REG_CODE.OUTHOS_STATUS
	,ALL_REG_CODE.F_DIAG
	,ALL_REG_CODE.I_CARD_TYPE
  INTO LDL_PERSON_CODE
  FROM LDL_REG_CODE  INNER JOIN ALL_REG_CODE 
  ON LDL_REG_CODE.REG_CODE=ALL_REG_CODE.REG_CODE

SELECT TOP 100 * FROM LDL_PERSON_CODE ORDER BY PERSON_CODE, DIAG_TIME
select distinct * into  LDL_PERSON_CODE_1 from  LDL_PERSON_CODE

SELECT TOP 100 * FROM LDL_PERSON_CODE_1 ORDER BY PERSON_CODE, DIAG_TIME

-------合并个人信息关联个人信息

SELECT LDL_PERSON_CODE_1.*
		,[PUB_PATIENT].BIRTHDAY
		,[PUB_PATIENT].SEX
		,[PUB_PATIENT].MASTER_INDEX
INTO LDL_PERSON_CODE_ALL
FROM LDL_PERSON_CODE_1 INNER  JOIN [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT]
ON [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT].PERSON_CODE =  LDL_PERSON_CODE_1.PERSON_CODE
ORDER BY MASTER_INDEX,DIAG_TIME ASC
SELECT TOP 100 * FROM LDL_PERSON_CODE_ALL
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL a
---- LDL_PERSON_CODE_ALL 人口学信息表

-----首诊"血脂异常"后的诊疗记录
drop table LDL_PERSON_CODE_ALL_FirstDiag
select a.* 
into LDL_PERSON_CODE_ALL_FirstDiag
from    LDL_PERSON_CODE_ALL  a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 

select top 10000 a.* from   LDL_PERSON_CODE_ALL_FirstDiag a 
order by   a.master_index,diag_TIME 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_FirstDiag a

--SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_FirstDiag_1 a

drop table  LDL_PERSON_CODE_ALL_FirstDiag_2times
SELECT *  into  LDL_PERSON_CODE_ALL_FirstDiag_2times --过滤 住院次数大于2次
FROM  LDL_PERSON_CODE_ALL_FirstDiag where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   LDL_PERSON_CODE_ALL_FirstDiag   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>1)

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_FirstDiag_2times a -- 15640

--DROP TABLE #LDL_PERSON_CODE_ALL

Select distinct a.master_index from LDL_PERSON_CODE_ALL_FirstDiag_2times a
join (SELECT * from   LDL_LAB a where a.I_ITEM_ENAME like '%LPA%'   ) b  on a.REG_CODE=b.REG_CODE
---

------------------他汀组
drop table  TATING
select a.master_index 
into TATING
from (select distinct master_index from  LDL_PERSON_CODE_ALL_FirstDiag_2times) a
 join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from   LDL_PERSON_CODE_ALL_FirstDiag_2times) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX 

Select distinct  * From TATING  order by master_INDEX   --12899

--非他汀组
drop table NON_TATING 
select a.master_index 
into NON_TATING
from (select distinct master_index from   LDL_PERSON_CODE_ALL_FirstDiag_2times) a
left join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from   LDL_PERSON_CODE_ALL_FirstDiag_2times) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX
where b.master_INDEX is null 

Select distinct  * From NON_TATING  order by master_INDEX --2741




select distinct MASTER_INDEX from LPA a where a.MASTER_INDEX in
(select MASTER_INDEX from LPA  a group by MASTER_INDEX having count(DISTINCT REG_CODE)>1 )







----基于诊疗覆盖时间（年）对患者进行筛选
SELECT *
into LDL_PERSON_CODE_ALL_3year 
FROM  LDL_PERSON_CODE_ALL Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 3 --years
)
ORDER BY MASTER_INDEX 
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_3year  a -- 5648

SELECT *
into LDL_PERSON_CODE_ALL_2year 
FROM  LDL_PERSON_CODE_ALL Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 2 --years
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_2year  a 

SELECT *
into LDL_PERSON_CODE_ALL_1year 
FROM  LDL_PERSON_CODE_ALL Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 1 --years
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_1year  a 





--------基于诊疗次数（次）对患者进行筛选


SELECT *  into  LDL_PERSON_CODE_ALL_0 --过滤 住院次数大于4次
FROM  LDL_PERSON_CODE_ALL where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   LDL_PERSON_CODE_ALL   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>2)
SELECT TOP 1000 * FROM  LDL_PERSON_CODE_ALL_0 ORDER BY MASTER_INDEX,REG_CODE
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_0 a --17178


SELECT *  into  LDL_PERSON_CODE_ALL_1 --过滤 住院次数大于3次
FROM  LDL_PERSON_CODE_ALL where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   LDL_PERSON_CODE_ALL   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>2)
SELECT TOP 1000 * FROM  LDL_PERSON_CODE_ALL_1 ORDER BY MASTER_INDEX,REG_CODE
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_1 a --28187


SELECT *  into  LDL_PERSON_CODE_ALL_2 --过滤 住院次数大于2次
FROM  LDL_PERSON_CODE_ALL where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   LDL_PERSON_CODE_ALL   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>1)
SELECT TOP 1000 * FROM  LDL_PERSON_CODE_ALL_1 ORDER BY MASTER_INDEX,REG_CODE
SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_2 a -- 51888
--DROP TABLE #LDL_PERSON_CODE_ALL


----------------------------基于诊疗次数（次）和时间覆盖（年）对患者进行筛选
------------------
drop table  LDL_PERSON_CODE_ALL_3year_3times 
SELECT *
into LDL_PERSON_CODE_ALL_3year_3times 
FROM  LDL_PERSON_CODE_ALL_1 Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 3 --years
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_3year_3times   a 
---------------------
drop table LDL_PERSON_CODE_ALL_1_5year_3times 
SELECT *
into LDL_PERSON_CODE_ALL_1_5year_3times 
FROM  LDL_PERSON_CODE_ALL_1 Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 1.5 --years
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_1_5year_3times   a 
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
----LDL_PERSON_CODE_ALL_1_5year_2times 
SELECT *
into LDL_PERSON_CODE_ALL_1_5year_2times 
FROM  LDL_PERSON_CODE_ALL_2 Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 1.5 --years
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_1_5year_2times   a 

--首诊"血脂异常"后的诊疗记录且
select a.* 
into LDL_PERSON_CODE_ALL_3_1_5year_2times
from    LDL_PERSON_CODE_ALL_1_5year_2times  a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_1_5year_2times where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 

--首诊"血脂异常"前的诊疗记录

select a.* 
into LDL_PERSON_CODE_ALL_4_1_5year_2times
from    LDL_PERSON_CODE_ALL_1_5year_2times  a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_1_5year_2times where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time<min_time
order by a.master_index,diag_TIME 

--他汀组

select a.master_index 
into TATING
from (select distinct master_index from  LDL_PERSON_CODE_ALL_1_5year_2times) a
 join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from  LDL_PERSON_CODE_ALL_1_5year_2times) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX

Select distinct  * From TATING  order by master_INDEX   --13057

--非他汀组
drop table NON_TATING 
select a.master_index 
into NON_TATING
from (select distinct master_index from  LDL_PERSON_CODE_ALL_1_5year_2times) a
left join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from  LDL_PERSON_CODE_ALL_1_5year_2times) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX
where b.master_INDEX is null 

Select distinct  * From NON_TATING  order by master_INDEX  --3584


-------指标检查数据
drop table LDL_LAB
SELECT 
      [REG_CODE]
      ,[REPORT_TIME]
      ,[ASSAY_ITEM_CODE]
      ,[ASSAY_ITEM_NAME]
      ,[ITEM_ENAME]
      ,[UNIT]
      ,[RESULTS]
      ,[REFRANGE],I_ITEM_ENAME
  into LDL_LAB
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[ASSAY_REPORT] where 
 ( I_ITEM_ENAME like '%APO-A%' or
  I_ITEM_ENAME like '%APO-B%' or 
  I_ITEM_ENAME like '%CRP%' or
  I_ITEM_ENAME like '%HDL' or
  I_ITEM_ENAME like '%HDL-C%' or 
  I_ITEM_ENAME like '%LDL%' or
  I_ITEM_ENAME like '%LDL-C%' or
  I_ITEM_ENAME like '%LPA%' or
  I_ITEM_ENAME like '%TC%' or
  I_ITEM_ENAME like '%TG%' or
  I_ITEM_ENAME like '%VLDL%' ) and REG_SOURCE=2

  SELECT count(distinct [REG_CODE]) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[ASSAY_REPORT] where 
  I_ITEM_ENAME like '%LPA%'

 SELECT count(distinct [REG_CODE]) FROM LDL_LAB where 
  I_ITEM_ENAME like '%LPA%'



Select distinct a.master_index from LDL_PERSON_CODE_ALL_1_5year_2times a
join (SELECT * from   LDL_LAB a where a.I_ITEM_ENAME like '%LPA%'   ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX

Select distinct a.master_index,a.reg_code,SEX,a.ORG_CODE,b.*  from LDL_PERSON_CODE_ALL_1_5year_2times a
join (SELECT * from   LDL_LAB a where a.I_ITEM_ENAME like '%LPA%'   ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX,a.REG_CODE

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------


------诊疗记录覆盖时间大于2/1.5年
SELECT *
into LDL_PERSON_CODE_ALL_2_1 
FROM  LDL_PERSON_CODE_ALL_1 Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from LDL_PERSON_CODE_ALL_1 
group by master_index
 ) b on a.master_index=b.master_index
--where datediff(year,min_time,max_time) > 2 --大于2年
where datediff(year,min_time,max_time) > 1.5 --大于1.5年
)
ORDER BY MASTER_INDEX 

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_2 a -- 5648

SELECT count(distinct a.MASTER_INDEX) FROM LDL_PERSON_CODE_ALL_2_1 a -- 8277


 
--去除首诊"血脂异常"前的诊疗记录且
select a.* 
into LDL_PERSON_CODE_ALL_3
from    LDL_PERSON_CODE_ALL_2 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 

----1.5年
select a.* 
into LDL_PERSON_CODE_ALL_3_1
from    LDL_PERSON_CODE_ALL_2_1 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 


Select  a.* from LDL_PERSON_CODE_ALL_3 a order by a.master_index,diag_TIME asc

SELECT count(DISTINCT MASTER_INDEX )  from LDL_PERSON_CODE_ALL_3

---- 5648名"血脂异常"患者，#LDL_PERSON_CODE_ALL_3 为住院纳入的患者 2年


SELECT count(DISTINCT MASTER_INDEX )  from LDL_PERSON_CODE_ALL_3_1

---- 5648名"血脂异常"患者，#LDL_PERSON_CODE_ALL_3 为住院纳入的患者 1.5年






---------------------------检验数据
/****** Script for SelectTopNRows command from SSMS  ******/
SELECT
      [REG_CODE]
      ,[REPORT_TIME]
      ,[ASSAY_ITEM_CODE]
      ,[ASSAY_ITEM_NAME]
      ,[ITEM_ENAME]
      ,[UNIT]
      ,[RESULTS]
      ,[REFRANGE]
  into LDL_LAB
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[ASSAY_REPORT] where 
  ITEM_ENAME like '%LPA%' or
  ITEM_ENAME like '%TC%' or 
  ITEM_ENAME like '%LDL%' or
  ITEM_ENAME like '%TG%' or
  ITEM_ENAME like '%HDL%' or 
  ITEM_ENAME like '%CM%' or
  ITEM_ENAME like '%VLDL%' or
  ITEM_ENAME like '%IDL%' or
  ITEM_ENAME like '%apo%' 

drop table LPA_LAB
SELECT * 
into LPA_LAB
from  [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[ASSAY_REPORT]
where ITEM_ENAME like '%LPA%' --13232
or ITEM_ENAME like '%脂蛋白(a)%'
or ITEM_ENAME like '%LP(a)%'





Select distinct a.master_index,SEX,a.ORG_CODE  from LDL_PERSON_CODE_ALL_3 a
join (SELECT * from   LPA_LAB  ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX


Select distinct a.master_index,SEX,a.ORG_CODE  from LDL_PERSON_CODE_ALL_3_1 a
join (SELECT * from   LPA_LAB  ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX







----选择他汀的就诊记录

DROP TABLE LDL_DRUG
SELECT [REG_CODE]
      ,[ITEM_NAME]
      ,[UNITPRICE]
      ,[QUANTITY]
      ,[UNIT]
      ,[FEE]
      ,[EMP_NAME]
      ,[DEPT_CODE]
      ,[DEPT_NAME]
      ,[CHARGE_TIME]
       ,[MEDIC_AREA]
INTO LDL_DRUG
FROM DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_FEE] a
WHERE [ITEM_NAME] LIKE '%他汀%' and a.[REG_CODE] IN
(Select  b.[REG_CODE] from LDL_PERSON_CODE_ALL_3 b )
SELECT TOP 1000 * from LDL_DRUG



select a.master_index 
into #TATING
from (select distinct master_index from LDL_PERSON_CODE_ALL_3) a
 join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from LDL_PERSON_CODE_ALL_3) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX

Select distinct  * From #TATING  order by master_INDEX   --5061


Drop table NON_TATING
select a.master_index 
into NON_TATING
from (select distinct  master_index from LDL_PERSON_CODE_ALL_3) a
left join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from LDL_PERSON_CODE_ALL_3  ) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX
where b.master_INDEX is null 

Select distinct  * From #NON_TATING  order by master_INDEX  -- 587 



--------------------1.5年
select a.master_index 
into TATING
from (select distinct master_index from LDL_PERSON_CODE_ALL_3_1) a
 join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from LDL_PERSON_CODE_ALL_3_1  ) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX

Select distinct  * From #TATING  order by master_INDEX   -- 7205


Drop table NON_TATING
select a.master_index 
into NON_TATING
from (select distinct  master_index from LDL_PERSON_CODE_ALL_3_1) a
left join  (
select distinct a.MASTER_INDEX from (
select distinct reg_code,person_code,master_index from LDL_PERSON_CODE_ALL_3_1  ) a
join DB_DIAG_HYPERLIPEMIA_ZONG.[dbo].[INPAT_fee] b on a.reg_code=b.REG_CODE
where  [ITEM_NAME]  LIKE '%他汀%'
) b on  a.master_index=b.master_INDEX
where b.master_INDEX is null 

Select distinct  * From NON_TATING  order by master_INDEX  --  1072
-------------------------人口学信息 1.5 年

--nontating
Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from LDL_PERSON_CODE_ALL_3_1 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index
where a.MASTER_INDEX in 
(Select distinct  * From NON_TATING  )

--tating
Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from LDL_PERSON_CODE_ALL_3_1 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index
where a.MASTER_INDEX in 
(Select distinct  * From TATING  )




-------------------------人口学信息 2 年


Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from LDL_PERSON_CODE_ALL_3 a
join (select master_index,min(diag_time)min_time  from LDL_PERSON_CODE_ALL_2 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index


--nontating
Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from #LDL_PERSON_CODE_ALL_3 a
join (select master_index,min(diag_time)min_time  from #LDL_PERSON_CODE_ALL_2 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index
where a.MASTER_INDEX in 
(Select distinct  * From #NON_TATING  )

--tating
Select distinct a.master_index,SEX, datediff(year,a.birthday,min_time),ORG_CODE from #LDL_PERSON_CODE_ALL_3 a
join (select master_index,min(diag_time)min_time  from #LDL_PERSON_CODE_ALL_2 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) b  on a.master_index=b.master_index
where a.MASTER_INDEX in 
(Select distinct  * From #TATING  )


---------------------检查信息
--nontating
Select distinct a.master_index,a.DIAG_TIME, a.SEX,ORG_CODE,b.ITEM_ENAME,b.UNIT,b.RESULTS from  #LDL_PERSON_CODE_ALL a
join (SELECT * from  #LDL_LAB b where b.ITEM_ENAME like '%LPA%' ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX,a.DIAG_TIME asc


Select distinct a.master_index from #LDL_PERSON_CODE_ALL_3 a
join (SELECT * from  #LDL_LAB b where b.ITEM_ENAME like '%LPA%' ) b  on a.REG_CODE=b.REG_CODE
order by  a.MASTER_INDEX








-------------------------------住院患者END
------------------------------------------------------------------------临时代码

------------患者数据拼接他汀

SELECT a.*
      ,[ITEM_NAME]
      ,[UNITPRICE]
      ,[QUANTITY]
      ,[UNIT]
      ,[FEE]
      ,[EMP_NAME]
      ,[DEPT_CODE]
      ,[DEPT_NAME]
      ,[CHARGE_TIME]
       ,[MEDIC_AREA]
INTO #PERSON_LDL_DRUG
FROM #LDL_PERSON_CODE_ALL_3 a INNER JOIN  #LDL_DRUG b
ON  a.REG_CODE = b.REG_CODE

SELECT * FROM  #PERSON_LDL_DRUG order by master_index,diag_TIME 


SELECT COUNT(DISTINCT master_index) FROM  #PERSON_LDL_DRUG -- 5084


------------数据拼接非他汀组

SELECT a.*
	  ,b.[ITEM_NAME]
      ,b.[EACH_DOSE]
      ,b.[MEDI_DOSE_UNIT]
      ,b.[MEDIC_USE_QUANTITY]
      ,b.[MEDI_USE_UNIT]
	  ,b.[INTAKE_START_TIME]
      ,b.[INTAKE_END_TIME]
INTO #PERSON_LDL_NOT_DRUG
FROM #LDL_PERSON_CODE_ALL_3 a INNER JOIN  #LDL_NOT_DRUG b
ON  a.REG_CODE = b.REG_CODE

SELECT * 
FROM   #PERSON_LDL_NOT_DRUG


DROP TABLE #PERSON_LDL_NOT_DRUG_1
SELECT a.* 
INTO #PERSON_LDL_NOT_DRUG_1
FROM  #PERSON_LDL_NOT_DRUG a
where Not Exists
(SELECT  DISTINCT b.master_index ter FROM  #PERSON_LDL_DRUG b)


SELECT count(*) From #PERSON_LDL_NOT_DRUG


SELECT COUNT(DISTINCT master_index) FROM  #PERSON_LDL_NOT_DRUG -- 


-------------------------------------------------------------------------------------------

-------------------------------- 门诊患者

 --筛选/建立门诊患者诊断信息

  SELECT top 100 * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_DIAG]
  SELECT [REG_CODE]
      ,[ICD]
      ,[DISEASE_NAME]
      ,[DIAG_TIME]
      ,[ORG_CODE]
	  INTO #LDL_REG_CODE_OUT
	  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_DIAG]
  SELECT top 100 * FROM  #LDL_REG_CODE_OUT

------筛选/建立患者个人信息
SELECT TOP 100 * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_REG_INFO]
SELECT [REG_ID]
      ,[REG_CODE]
      ,[PERSON_CODE]
	  ,[CARD_ID]
	  ,F_DIAG
	  ,I_CARD_TYPE
  INTO #ALL_REG_CODE_OUT -- 
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_REG_INFO]
SELECT TOP 100 * FROM #ALL_REG_CODE_OUT

-------合并个人信息 
DROP TABLE #LDL_PERSON_CODE_OUT

SELECT a.*
	,b.[REG_ID]
	,b.[PERSON_CODE]
	,b.[CARD_ID]
	,b.F_DIAG
	,b.I_CARD_TYPE
  INTO #LDL_PERSON_CODE_OUT
  FROM #LDL_REG_CODE_OUT a INNER JOIN #ALL_REG_CODE_OUT b
  ON a.REG_CODE=b.REG_CODE

SELECT TOP 100 * FROM #LDL_PERSON_CODE_OUT ORDER BY PERSON_CODE, DIAG_TIME
DROP TABLE #LDL_PERSON_CODE_OUT
select distinct * into  #LDL_PERSON_CODE_OUT_1 from  #LDL_PERSON_CODE_OUT

DROP TABLE #LDL_PERSON_CODE_OUT
SELECT TOP 100000 * FROM #LDL_PERSON_CODE_OUT_1 -- where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
ORDER BY PERSON_CODE, DIAG_TIME

-------合并个人信息关联个人信息

SELECT #LDL_PERSON_CODE_OUT_1.*
		,[PUB_PATIENT].BIRTHDAY
		,[PUB_PATIENT].SEX
		,[PUB_PATIENT].MASTER_INDEX
INTO #LDL_PERSON_CODE_ALL_OUT
FROM #LDL_PERSON_CODE_OUT_1 INNER  JOIN [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT]
ON [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT].PERSON_CODE =  #LDL_PERSON_CODE_OUT_1.PERSON_CODE
ORDER BY MASTER_INDEX,DIAG_TIME ASC
SELECT TOP 10000 * FROM #LDL_PERSON_CODE_ALL


SELECT *  into   #LDL_PERSON_CODE_ALL_OUT_1 --过滤
FROM  #LDL_PERSON_CODE_ALL where MASTER_INDEX IN
( SELECT MIN(MASTER_INDEX) FROM   #LDL_PERSON_CODE_ALL   GROUP BY MASTER_INDEX HAVING count(DISTINCT REG_CODE)>3)
SELECT TOP 1000 * FROM #LDL_PERSON_CODE_ALL_OUT_1 ORDER BY MASTER_INDEX,REG_CODE


DROP TABLE #LDL_PERSON_CODE_ALL_1



SELECT *
into #LDL_PERSON_CODE_ALL_OUT_2 
FROM  #LDL_PERSON_CODE_ALL_OUT_1 Where  master_index IN
(
select a.master_index from (
select master_index,min(diag_time)min_time  from #LDL_PERSON_CODE_ALL_OUT_1 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index ) a 
join (
 select master_index,max(diag_time)max_time  from #LDL_PERSON_CODE_ALL_OUT_1 
group by master_index
 ) b on a.master_index=b.master_index
where datediff(year,min_time,max_time) > 2
)
ORDER BY MASTER_INDEX 


 

select a.* 
into #LDL_PERSON_CODE_ALL_OUT_3
from    #LDL_PERSON_CODE_ALL_OUT_2 a
join (select master_index,min(diag_time)min_time  from #LDL_PERSON_CODE_ALL_OUT_2 where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'
group by master_index) b  on a.master_index=b.master_index
where a.diag_time>=min_time
order by a.master_index,diag_TIME 

Select  a.* from #LDL_PERSON_CODE_ALL_OUT_3 a order by a.master_index,diag_TIME asc



--------------------------------他汀用药组

DROP TABLE #LDL_DRUG_OUT
SELECT [REG_CODE]
      ,[PRES_TIME]
      ,[PRES_TYPE]
      ,[MEDIC_CLASS]
      ,[MEDIC_GENERAL_NAME]
      ,[MEDIC_SPEC]
      ,[MEDIC_AREA]
      ,[MEDIC_USE_QUANTITY]
      ,[MEDIC_USE_UNIT]
      ,[MEDIC_USE_CODE]
      ,[MEDIC_USE_MODE]
      ,[MEDIC_DAYS]
      ,[MEDIC_QUANTITY]
      ,[MEDIC_UNIT]
INTO #LDL_DRUG_OUT
FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_PRES] a
WHERE [MEDIC_GENERAL_NAME] LIKE '%他汀%' and a.[REG_CODE] IN
(Select  b.[REG_CODE] from  #LDL_PERSON_CODE_ALL_OUT_3 b )
SELECT TOP 1000 * from #LDL_DRUG_OUT

--------------------------------非他汀用药组
DROP TABLE #LDL_DRUG_NOT_OUT
SELECT [REG_CODE]
      ,[PRES_TIME]
      ,[PRES_TYPE]
      ,[MEDIC_CLASS]
      ,[MEDIC_GENERAL_NAME]
      ,[MEDIC_SPEC]
      ,[MEDIC_AREA]
      ,[MEDIC_USE_QUANTITY]
      ,[MEDIC_USE_UNIT]
      ,[MEDIC_USE_CODE]
      ,[MEDIC_USE_MODE]
      ,[MEDIC_DAYS]
      ,[MEDIC_QUANTITY]
      ,[MEDIC_UNIT]
INTO #LDL_DRUG_NOT_OUT
FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[OUT_PRES] a
WHERE [MEDIC_GENERAL_NAME] NOT LIKE '%他汀%' and a.[REG_CODE] IN
(Select  b.[REG_CODE] from  #LDL_PERSON_CODE_ALL_OUT_3 b )
SELECT TOP 1000 * from #LDL_DRUG_NOT_OUT


------------数据拼接他汀组
Drop table #PERSON_LDL_DRUG_OUT
SELECT a.*
      ,b.[PRES_TIME]
      ,b.[PRES_TYPE]
      ,b.[MEDIC_CLASS]
      ,b.[MEDIC_GENERAL_NAME]
      ,b.[MEDIC_SPEC]
      ,b.[MEDIC_AREA]
      ,b.[MEDIC_USE_QUANTITY]
      ,b.[MEDIC_USE_UNIT]
      ,b.[MEDIC_USE_CODE]
      ,b.[MEDIC_USE_MODE]
      ,b.[MEDIC_DAYS]
      ,b.[MEDIC_QUANTITY]
      ,b.[MEDIC_UNIT]
INTO #PERSON_LDL_DRUG_OUT
FROM #LDL_PERSON_CODE_ALL_OUT_3 a INNER JOIN  #LDL_DRUG_OUT b
ON  a.REG_CODE = b.REG_CODE

SELECT * FROM  #PERSON_LDL_DRUG_OUT order by master_index,diag_TIME asc


SELECT COUNT(DISTINCT master_index) FROM  #PERSON_LDL_DRUG_OUT -- 84


------------数据拼接非他汀组
Drop table #PERSON_LDL_NOT_DRUG_OUT
SELECT a.*
      ,b.[PRES_TIME]
      ,b.[PRES_TYPE]
      ,b.[MEDIC_CLASS]
      ,b.[MEDIC_GENERAL_NAME]
      ,b.[MEDIC_SPEC]
      ,b.[MEDIC_AREA]
      ,b.[MEDIC_USE_QUANTITY]
      ,b.[MEDIC_USE_UNIT]
      ,b.[MEDIC_USE_CODE]
      ,b.[MEDIC_USE_MODE]
      ,b.[MEDIC_DAYS]
      ,b.[MEDIC_QUANTITY]
      ,b.[MEDIC_UNIT]
INTO #PERSON_LDL_NOT_DRUG_OUT
FROM #LDL_PERSON_CODE_ALL_OUT_3 a INNER JOIN  #LDL_NOT_DRUG b
ON  a.REG_CODE = b.REG_CODE

SELECT TOP 1000 * FROM  #PERSON_LDL_NOT_DRUG order by master_index,diag_TIME 


SELECT COUNT(DISTINCT master_index) FROM  #PERSON_LDL_NOT_DRUG -- 5644




----------------------------END




DROP TABLE  #LDL_PERSON_CODE_ALL_2 

SELECT top 1000 * from #LDL_PERSON_CODE_ALL_2 ORDER BY MASTER_INDEX,DIAG_Time asc




SELECT TOP 1000  [REG_ID]
      ,[REG_CODE]
      ,[PERSON_CODE] FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_REG_INFO]


SELECT *  into  #LDL_PERSON_CODE_2 --过滤
FROM  #LDL_PERSON_CODE_1 where PERSON_CODE IN
(SELECT #LDL_PERSON_CODE_1.PERSON_CODE  FROM  #LDL_PERSON_CODE_1  GROUP BY #LDL_PERSON_CODE_1.PERSON_CODE HAVING count(PERSON_CODE)>3)
ORDER by PERSON_CODE, DIAG_TIME asc
 
 
 
 
 
 
 
 
 
 
 
 
 
 SELECT SEX,count(*) FROM [DB_DIAG_DIABETES_ZONG].[dbo].[PUB_PATIENT] group by SEX 


 SELECT count(*) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG] where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'

 SELECT * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG] where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'

 SELECT count(*) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].OUT_REG_INFO where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%' or ICD like '%E78%'

 SELECT * FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].OUT_DIAG 





 SELECT count(*) FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG] where DISEASE_NAME like '%血%脂%' or DISEASE_NAME like '%脂%血%'


 SELECT  count(*)
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[PUB_PATIENT]   



 --筛选患者诊断信息
 SELECT [REG_CODE]
      ,[ICD]
      ,[DISEASE_NAME]
      ,[DIAG_TIME]
      ,[ORG_CODE]
	   FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG]

DROP TABLE #LDL_REG_CODE

 SELECT [REG_CODE]
      ,[ICD]
      ,[DISEASE_NAME]
      ,[DIAG_TIME]
      ,[ORG_CODE]
  INTO #LDL_REG_CODE
  FROM [DB_DIAG_HYPERLIPEMIA_ZONG].[dbo].[INPAT_DIAG]
  WHERE [DISEASE_NAME] like '%血%脂%' or
        [DISEASE_NAME] like '%脂%血%' or
        [ICD] like '%E78%'

SELECT * FROM #LDL_REG_CODE

------筛选患者个人信息
SELECT [REG_ID]
      ,[REG_CODE]
      ,[PERSON_CODE]
  INTO #ALL_REG_CODE 
  FROM [DB_DIAG_DIABETES_ZONG].[dbo].[INPAT_REG_INFO]
SELECT TOP 1000 * FROM #ALL_REG_CODE 


-------------合并表1和表2 as 表3

SELECT #LDL_REG_CODE.* 
   ,#ALL_REG_CODE.[PERSON_CODE]
  INTO #LDL_PERSON_CODE
  FROM #LDL_REG_CODE  INNER JOIN #ALL_REG_CODE 
  ON #LDL_REG_CODE.REG_CODE=#ALL_REG_CODE.REG_CODE
SELECT TOP 1000 * FROM #LDL_PERSON_CODE ORDER BY PERSON_CODE, DIAG_TIME

-------------完成患者信息数据 as 表4
SELECT #LDL_PERSON_CODE.* 
   ,PUB_PATIENT.SEX
   ,PUB_PATIENT.BIRTHDAY
  INTO #LDL_PERSON_CODE_1 -- 人员信息表
  FROM  #LDL_PERSON_CODE  INNER JOIN [DB_DIAG_DIABETES_ZONG].[dbo].[PUB_PATIENT] 
  ON #LDL_PERSON_CODE.PERSON_CODE= PUB_PATIENT.PERSON_CODE

SELECT *  into  #LDL_PERSON_CODE_2 --过滤
FROM  #LDL_PERSON_CODE_1 where PERSON_CODE IN
(SELECT #LDL_PERSON_CODE_1.PERSON_CODE  FROM  #LDL_PERSON_CODE_1  GROUP BY #LDL_PERSON_CODE_1.PERSON_CODE HAVING count(PERSON_CODE)>3)
ORDER by PERSON_CODE, DIAG_TIME asc

 


--------检查结果表 as 表5 
Drop table #LDL_LAB
SELECT [REG_CODE]
		,REPORT_TIME
		,[ITEM_ENAME]
		,RESULTS
INTO #LDL_LAB
FROM [DB_DIAG_DIABETES_ZONG].[dbo].[ASSAY_REPORT] 
WHERE [ITEM_ENAME] LIKE '%LPA%' or [ITEM_ENAME]  LIKE 'LDL%'
SELECT top 100 * from #LDL_LAB

--------他汀用药结果表 as 表6
SELECT [REG_CODE]
      ,[ITEM_NAME]
      ,[EACH_DOSE]
      ,[MEDI_DOSE_UNIT]
      ,[MEDIC_USE_QUANTITY]
      ,[MEDI_USE_UNIT]
	  ,[INTAKE_START_TIME]
      ,[INTAKE_END_TIME]
INTO #LDL_DRUG
FROM [DB_DIAG_DIABETES_ZONG].[dbo].[INPAT_ORDER] 
WHERE [ITEM_NAME] LIKE '%他汀%'
SELECT top 100 * from #LDL_DRUG



SELECT  top 100 * FROM #LDL_LAB,#LDL_PERSON_CODE_2 where #LDL_LAB.REG_CODE = #LDL_PERSON_CODE_2.REG_CODE









 

