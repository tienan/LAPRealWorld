#tools

trendAnalysis = function(rawData,sign){

  df_lpa = rawData
  df_lpa$sex = as.character(df_lpa$sex)
  #男1女2其他
  df_lpa[grepl(pattern = "男",x=df_lpa$sex),]$sex = 1
  df_lpa[grepl(pattern = "女",x=df_lpa$sex),]$sex = 2
  df_lpa[!df_lpa$sex%in%c(1,2),]$sex = 3
  df_lpa$RESULTS=as.numeric(as.character(df_lpa$RESULTS))
  df_lpa = na.omit(df_lpa)
  df_lpa$order =c(1:nrow(df_lpa))
  by_master_index <- group_by(df_lpa, MASTER_INDEX)
  nrow(by_master_index)
  models <- by_master_index %>% do(mod = lm( RESULTS~order+age, data = .))
  tatingSign = sign(as.data.frame(summarise(models, rsq = as.data.frame(summary(mod)$coefficients)[2,1])))
  table(tatingSign)
  #tatingSign
  #-1    1 
  #955 2166 
  #1306 1815 年龄调整后
  #1851/1306
  basicData <- by_master_index %>% 
    summarise(mean= median(RESULTS),firstValue=first(RESULTS),lastValue= last(RESULTS),
              age=min(age),ORG_CODE=first(ORG_CODE),gender=first(sex))
  res = basicData
  res$tating = sign
  res$outcome = tatingSign
  res =as.data.frame(res)
  return(res)
}

drugAnalysis = function(){
  
}

