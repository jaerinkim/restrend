## 사업장과 업태구분명을 키워드로 입력하면 행정안전부에서 제공하는 일반, 휴게음식점 표준데이터에서 해당업종의 일간 업체 수를 출력하는 프로그램입니다.
## 예)'마라'가 사업장명에 들어간 '중국식' 업태 업체를 입력하면 2022년 1월 1일, 2일,... 에 서초동, 보광동,... 에 몇 개의 마라 음식점이 있었는지 출력합니다.
## Author : 김재린

import pandas as pd

generalPath = "./data/fulldata_07_24_04_P_일반음식점.csv"
smallPath = "./data/fulldata_07_24_05_P_휴게음식점.csv"

general = pd.read_csv(generalPath, encoding='CP949')
small = pd.read_csv(smallPath, encoding='CP949')

## 날짜를 timedate type으로 변환
general.dropna(subset=['인허가일자'], inplace=True)
general.loc[general['폐업일자'].isna(),'폐업일자'] = '2099-12-31'
small.dropna(subset=['인허가일자'], inplace=True)
small.loc[small['폐업일자'].isna(),'폐업일자'] = '2099-12-31'

datelist = ['인허가일자','폐업일자']
dfs = [general,small]
d29 = [2]
d30 = [4,6,9,11]
d31 = [1,3,5,7,8,10,12]

for i in datelist:
    for j in dfs:
        ## YYYY-MM-DD를 YYYYMMDD로 수정함
        j[i] = j[i].str.replace('-','')
        small[i] = j[i].str.replace('-','')
        ## None 날짜를 19000101로 수정함
        j.loc[j[i].isna(),i] = '19000101'
        small.loc[small[i].isna(),i] = '19000101'    
        ##날짜 중 2월31일 등 존재하지 않는 날짜를 수정함
        wrong29 = (j[i].str[4:6].astype(int).isin(d29)) & (j[i].str[6:].astype(int)>28)
        j.loc[wrong29,i] = j.loc[wrong29,i].str[:6]+'28'
        wrong30 = (j[i].str[4:6].astype(int).isin(d30)) & (j[i].str[6:].astype(int)>30)
        j.loc[wrong30,i] = j.loc[wrong30,i].str[:6]+'30'
        wrong31 = (j[i].str[4:6].astype(int).isin(d31)) & (j[i].str[6:].astype(int)>31)
        j.loc[wrong31,i] = j.loc[wrong31,i].str[:6]+'31'
        wrong13 = j[i].str[4:6].astype(int)>12
        j.loc[wrong13,i] = j.loc[wrong13,i].str[:4]+'0615'
        wrong0 = j[i].str[4:6].astype(int)==0
        j.loc[wrong0,i] = j.loc[wrong0,i].str[:4] + '01' + j.loc[wrong0,i].str[6:]
        wrong00 = j[i].str[6:].astype(int)==0
        j.loc[wrong00,i] = j.loc[wrong00,i].str[:6] + '01'
        ## datetime 형식으로 변환
        j[i] = pd.to_datetime(j[i], format='%Y%m%d')

df = general._append(small)
df.dropna(subset='사업장명', inplace=True)
df['광역'] = df['소재지전체주소'].str.split(' ').str[0]
df['시군구'] = df['소재지전체주소'].str.split(' ').str[1]
df['읍면동'] = df['소재지전체주소'].str.split(' ').str[2]

df.loc[df['광역']=="세종특별자치시",'읍면동'] = df.loc[df['광역']=="세종특별자치시",'시군구']
df.loc[df['광역']=="세종특별자치시",'시군구'] = "세종특별자치시"

## 일관성을 위해 일관되지 않은 주소들은 검색에서 제외했습니다. (예: 한국마사회)
df = df[df['광역'].isin(df['광역'].unique()[:16])]

df['지역'] = df['광역'] + ' ' + df['시군구'] + ' ' + df['읍면동']

## date부터 enddate까지 모든 날짜를 리스트로 출력. date와 endddate는 timestamp여야 함.
def dates(date,enddate):
    out = []
    x = date
    flag = True
    while flag:
        out.append(x)
        x = x + pd.DateOffset(1)
        if x > enddate:
            flag = False
    return(out)


## keywords는 검색어 list임.
## 추후 다른 조건이 추가될 시 argument를 추가하면 됨.

## keywords에 포함된 string이 사업장명에 포함된 업소들을 출력함.
## 단, type에 포함된 string이 사업장명에 포함된 업소에 한정함.
def search(df,keywords,type):
    out = df[df['사업장명'].str.contains('|'.join(keywords))]
    return(out[out['업태구분명'].str.contains('|'.join(type))])

## dataframe을 입력하면 첫 인허가일자를 출력함.
def findStartDate(df):
    return(df['인허가일자'].min())

## prev는 영업중인 업소를 나타내는 boolean으로 처음에는 False만으로 이루어진 series를 입력함.
## date는 개업 및 폐업을 계산할 날짜를 나타냄

## 주어진 boolean(prev)을 기반으로 특정일에 개업 및 폐업한 업소를 고려하여 해당일 영업중인 업소를 boolean으로 출력함.
def getNew(df, prev, date):
    out = prev
    out = out | (df.인허가일자==date)
    out = out ^ (df.폐업일자==date)
    return(out)

## 동면읍과 년월일의 cartesian product 리스트를 출력함
## startDate는 조사를 시작할 날짜의 timestamp
def genRegion(df):
    regions = df['지역'].unique()
    return(regions)
## startDate부터 endDate까지 매일을 출력함.
def genDates(startDate,endDate):
    out = []
    while startDate < endDate:
        out.append(startDate)
        startDate = startDate + pd.DateOffset(1)
    return(out)

## region 지역에서 업체가 하나라도 존재했던 시점부터 enddate까지 매일 업체의 수와 해당 날짜를 출력함

def genShops(df,region,endDate = pd.to_datetime('20240512')):
    df = df[df['지역']==region]
    startDate = df['인허가일자'].min()
    prev = (df['인허가일자'] == '0')
    out = [[],[]]
    while startDate <= endDate:
        prev = getNew(df,prev,startDate)
        out[0].append(sum(prev))
        out[1].append(startDate)
        startDate = startDate + pd.DateOffset(1)
    return(out)

## 조건에 맞는 업체가 하나라도 존재했던 시점부터 endDate까지 매일 업체의 수를 DataFrame으로 출력함
## keywords에 포함된 string이 사업장명에 포함된 업소들을 출력함.
## 단, type에 포함된 string이 사업장명에 포함된 업소에 한정함.

def shopDates(df,keywords,type,endDate = pd.to_datetime('20240512')):
    df = search(df=df,keywords=keywords,type=type)
    out = pd.DataFrame([],columns=['지역',keywords[0]])
    regions = genRegion(df)
    dates = genDates(findStartDate(df),endDate)
    dates = pd.DataFrame(dates)
    for i in regions:
        y = genShops(df,i)
        dates = pd.DataFrame(y[1])
        index = i + '_' + dates[0].dt.date.astype(str)
        x = pd.DataFrame(y[0],index=index,columns=[keywords[0]])
        x['지역'] = x.index
        print(x)
        print(out)
        out = out._append(x)
    return(out)


## 마라 예시
mala = shopDates(df,['마라'],['중국식','일반조리판매'])
mala['광역'] = mala['지역'].str.split(' ').str[0]
mala['시군구'] = mala['지역'].str.split(' ').str[1]
mala['읍면동'] = mala['지역'].str.split(' ').str[2].str[:-11]
mala.to_csv("마라예시.csv",encoding='EUC-KR')