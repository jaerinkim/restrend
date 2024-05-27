## 사업장과 업태구분명을 키워드로 입력하면 행정안전부에서 제공하는 일반, 휴게음식점 표준데이터에서 해당업종의 일간 업체 수를 출력하는 프로그램입니다.
## 예)'마라'가 사업장명에 들어간 '중국식' 업태 업체를 입력하면 2022년 1월 1일, 2일,... 에 서초동, 보광동,... 에 몇 개의 마라 음식점이 있었는지 출력합니다.
## 지원 예정: 월별 및 연간 결과 출력, 위치 정보 출력, 가게가 한 번도 없었던 지역의 데이터를 삽입.
## Author : 김재린

import pandas as pd

## generalPath에 일반음식점 smallPath에 휴게음식점 경로를 입력함.
## 전처리된 DataFrame을 출력함.
def init(generalPath = "../../data/fulldata_07_24_04_P_일반음식점.csv", smallPath = "../../data/fulldata_07_24_05_P_휴게음식점.csv"):
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

    df = pd.concat([dfs[0],dfs[1]])
    df.dropna(subset='사업장명', inplace=True)
 
    df['광역'] = df['소재지전체주소'].str.split(' ').str[0]
    df['시군구'] = df['소재지전체주소'].str.split(' ').str[1]
    df['읍면동'] = df['소재지전체주소'].str.split(' ').str[2]
    df.loc[df['광역']=="세종특별자치시",'읍면동'] = df.loc[df['광역']=="세종특별자치시",'시군구']
    df.loc[df['광역']=="세종특별자치시",'시군구'] = ""
    
    ## 일관성을 위해 일관되지 않은 주소들은 검색에서 제외했습니다. (예: 한국마사회)
    rglist=['전북특별자치도', '충청북도', '경기도', '강원특별자치도', '대구광역시', '서울특별시', '광주광역시', '전라남도',
            '경상북도', '인천광역시', '부산광역시', '대전광역시', '충청남도', '경상남도', '울산광역시',
            '제주특별자치도', '세종특별자치시']
        
    df = df[df['광역'].isin(rglist)]

    ## 광역시가 아닌 시의 읍면동 정보를 정확하게 수정함.
    temp = df['소재지전체주소'].str.split(' ').str[3]
    temp1 = temp.str.extract(r'(^[가-힣]+[동,면,읍,리]$)')
    temp2 = df.읍면동.str.extract(r'^([가-힣]+구)$')
    tbool = ~(temp1.isna()[0] | temp2.isna()[0])
    df.loc[tbool,'시군구'] = df.loc[tbool,'시군구'] + " " + df.loc[tbool,'읍면동']
    df.loc[tbool,'읍면동'] = temp1.loc[tbool,0]
    df['지역'] = df['광역'] + ' ' + df['시군구'] + ' ' + df['읍면동']
    df.loc[df.광역=='세종특별자치시','지역'] = df.loc[df.광역=='세종특별자치시','광역'] + " " + df.loc[df.광역=='세종특별자치시','읍면동']
    return(df)


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
## 단, rtype에 포함된 string이 사업장명에 포함된 업소에 한정함.
def search(df,keywords,rtype):
    if len(keywords)==0:
        if len(rtype)==0:
            return(df)
        else:
            return(df[df['업태구분명'].str.contains('|'.join(rtype))])
    else:
        out = df[df['사업장명'].str.contains('|'.join(keywords))]
        if len(rtype)==0:
            return(out)
        else:
            return(out[out['업태구분명'].str.contains('|'.join(rtype))])

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
    regions = pd.Series(df['지역'].unique()).dropna()
    return(regions)

## startDate부터 endDate까지 매일을 출력함.
def genDates(startDate,endDate):
    out = []
    while startDate < endDate:
        out.append(startDate)
        startDate = startDate + pd.DateOffset(1)
    return(out)

## region 지역에서 업체가 하나라도 존재했던 시점부터 enddate까지 매일 업체의 수와 해당 날짜를 출력함

def genShops(df,region,startDate=pd.to_datetime('20040301'),endDate = pd.to_datetime('20240331')):
    df = df[df['지역']==region]
    startDate = max(df['인허가일자'].min(),startDate)
    prev = (df['인허가일자'] < startDate) & ~(df['폐업일자']< startDate)
    out = [[],[]]
    while startDate <= endDate:
        prev = getNew(df,prev,startDate)
        out[0].append(sum(prev))
        out[1].append(startDate)
        startDate = startDate + pd.DateOffset(1)
    return(out)


## 조건에 맞는 업체가 하나라도 존재했던 시점이나 startDate 중 늦은 시점부터 endDate까지 매일 업체의 수를 DataFrame으로 출력함
## keywords에 포함된 string이 사업장명에 포함된 업소들을 출력함.
## 단, rtype에 포함된 string이 사업장명에 포함된 업소에 한정함.

def shopDates(df,keywords,rtype,startDate=pd.to_datetime('20040331'),endDate = pd.to_datetime('20240331')):
    df = search(df=df,keywords=keywords,rtype=rtype)
    out = pd.DataFrame([],columns=['지역','광역','시군구','읍면동','연월일',keywords[0]])
    regions = genRegion(df)
    dates = genDates(max(startDate,findStartDate(df)),endDate)
    dates = pd.DataFrame(dates)
    for i in regions:
        y = genShops(df,i)
        dates = pd.DataFrame(y[1])
        if len(dates)!=0:
            print(f'Processing {keywords[0]} at {i}')
            index = i + '_' + dates[0].dt.date.astype(str)
            x = pd.DataFrame(y[0],index=index,columns=[keywords[0]])
            temp = df[df.지역==i].iloc[0]
            x['지역'] = temp.지역
            x['광역'] = temp.광역
            x['시군구'] = temp.시군구
            x['읍면동'] = temp.읍면동
            x['연월일'] = x.index.str[-10:]
            out = pd.concat([out,x])
        else:
            next
    return(out)


# 성능상의 문제가 있을시 이 버전 사용.
# def shopDates(df,keywords,rtype,startDate=pd.to_datetime('20040331'),endDate=pd.to_datetime('20240331')):
#     allRegions = genRegion(df)
#     df = search(df=df,keywords=keywords,rtype=rtype)
#     regions = genRegion(df)
#     dates = genDates(max(startDate,findStartDate(df)),endDate)
#     dates = pd.Series(dates).dt.strftime('%Y-%m-%d')
#     temp = pd.DataFrame(index=allRegions)
#     name = keywords[0]
#     temp[name] = 0
#     print(f'Initializing {name}')
#     opened = df[df.인허가일자<=startDate].지역.dropna()
#     for i in opened:
#         temp.loc[temp.index==i,name] += 1
#     closed = df[df.폐업일자<=startDate].지역.dropna()
#     for i in closed:
#         temp.loc[temp.index==i,name] -= 1
#     temp.index = temp.index + '_' + dates[0]
#     out = temp.copy()
        
#     for i in dates[1:]:
#         temp.index = temp.index.str[:-11]
#         opened = df[df.인허가일자==i].index
#         for j in opened:
#             temp.loc[temp.index==j,name] += 1
#             print(f'{name} 인허가: {i}')
#         closed = df[df.폐업일자==i].index
#         for j in closed:
#             temp.loc[temp.index==j,name] -= 1
#             print(f'{name} 폐업: {i}')
#         temp.index = temp.index + '_' + i
#         out = pd.concat([out, temp])
#     return(out)


def regionToTab(regions):
    out = pd.DataFrame(index=regions, columns=[])
    out['광역'] = out.index.str.split(' ').str[0]
    out['시군구'] = out.index.str.split(' ').str[1]
    out['읍면동'] = out.index.str.split(' ').str[2]
    temp = out.index.str.split(' ').str[3]
    tbool = ~temp.isna()
    out.loc[tbool,'시군구'] = out.loc[tbool,'시군구'] + " " + out.loc[tbool,'읍면동']
    out.loc[tbool,'읍면동'] = temp[tbool]
    return(out)

def listToCsv(df,searchList,startDate = pd.to_datetime('20040331'),endDate = pd.to_datetime('20240331')):
    for i in searchList:
        out = shopDates(df,i[0],i[1],startDate,endDate)
        out.to_csv(i[0][0] + ".csv", encoding="EUC-KR")
    return()

def csvToOut(restname):
    out = pd.read_csv(restname+".csv",index_col=0)
    return(out)

def getRecent(df,keywords,rtype):
    temp = search(df,keywords,rtype)
    temp = temp[temp.상세영업상태명=="영업"]
    temp = temp.groupby(['광역','시군구','읍면동']).size()
    return(temp)

## 마라 예시
# mala = shopDates(df,['마라'],['중국식','일반조리판매'])
# mala.to_csv("마라예시.csv",encoding='EUC-KR')

# df의 시간을 제한하는 함수
# shopDates에 옵션으로 넣을 것을 고려
def timeLimit(df):
    out=pd.to_datetime(df['연월일'])>pd.to_datetime('20201231')
    return(out)