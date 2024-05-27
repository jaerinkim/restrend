import extract as ex
import pandas as pd
# from konlpy.tag import Okt

df = ex.init()
searchList = [[['마라'],['중국식','일반조리판매']], [['탕후루'],[]],[['카스테라'],[]],[['떡볶이'],[]],[['포케'],[]],[['양꼬치'],[]],[['곱창'],[]],[['핫도그'],[]],[['타코야끼'],[]],
              [['훠궈'],[]],[['빙수'],[]],[['설빙'],[]],[['요거트'],[]],[['밥버거'],[]],[['베이글'],[]],[['닭발'],[]],[['버블티'],[]],[['공차'],[]],[['닭강정'],[]],
              [['와플'],[]],[['토스트'],[]],[['파스타'],[]],[['샐러드'],[]],[['십원빵'],[]],[['도넛'],[]],[['마카롱'],[]],[['크로플'],[]],[['맥도날드'],[]],[['롯데리아'],[]],
              [['버거킹'],[]],[['서브웨이'],[]],[['케이에프씨'],[]],[['한솥',],[]],[['푸라닭'],[]],[['엽기떡볶이'],[]],[['역전할머니맥주'],[]],[['지코바'],[]]]

slist = ['식당', '치킨', '호프', '분식', '갈비', '칼국수', '피자', '포차', '김밥', '횟집', '통닭', '해장국', '포장마차', '보쌈', '감자탕', '돈까스',
         '국밥', '바베큐', '순대', '냉면', '꼬치', '추어탕', '닭갈비', '오리', '비어', '아구찜', '한우', '삼겹살']

# rlist = searchList
for i in slist:
    searchList.append([[i],[]])
    
slist = []
for i in searchList:
    slist.append(i[0][0])    

ex.listToCsv(df, searchList)

# 최빈명사 검색
# x = df.사업장명.str.cat()
# result1 = Okt().nouns(x[:int(len(x)/3)])
# result2 = Okt().nouns(x[int(len(x)/3):2*int(len(x)/3)])
# result3 = Okt().nouns(x[2*int(len(x)/3):])
# result = result1+result2+result3
# pd.Series(result).value_counts().to_csv("최빈명사.csv", encoding="EUC-KR")

# 메모리 문제로 데이터셋을 둘로 나눔
# searchList2 = searchList[37:]
# searchList = searchList[:37]

dflist = []
for i in searchList:
    print(f'Appending {i[0][0]}')
    dflist.append(pd.read_csv(i[0][0]+".csv",index_col=0,encoding="EUC-KR"))

flag = False
for i in range(len(dflist)):
    read = dflist[i].groupby(['광역','시군구','읍면동']).max(searchList[i])
    if flag:
        out = pd.concat([out,read], axis=1)
    else:
        out = read
        flag = True
out.fillna(0, inplace=True)
out.columns = out.columns + "max"
out.to_csv("지역별최대치.csv",encoding="EUC-KR")

flag = False
for i in range(len(dflist)):
    name = searchList[i][0][0]
    read = dflist[i][dflist[i].index.str[-10:]=='2024-03-31']
    read.index = [read.광역,read.시군구,read.읍면동]
    read = read[name]
    if flag:
        out = pd.concat([out,read], axis=1)
    else:
        out = read
        flag = True
out.fillna(0, inplace=True)
out.columns = out.columns + "recent2"
out.to_csv("지역별최근값2.csv",encoding="EUC-KR")

flag = False
for i in range(len(dflist)):
    name = searchList[i][0][0]
    read = dflist[i][pd.to_datetime(dflist[i].연월일)>pd.to_datetime("2004-03-31")]
    read1 = read.groupby(['광역','시군구','읍면동']).idxmin(numeric_only=name)
    read2 = read.groupby(['광역','시군구','읍면동']).idxmax(numeric_only=name)
    read1[name] = read1[name].str[-10:]
    read2[name] = read2[name].str[-10:]
    if flag:
        out = pd.concat([out,read1], axis=1)
        out2 = pd.concat([out2,read2], axis=1)
    else:
        out = read1
        out2 = read2
        flag = True
out.columns = out.columns + "minDate"
out2.columns = out2.columns + "maxDate"
out.to_csv("지역별최소치일자.csv",encoding="EUC-KR")
out2.to_csv("지역별최대치일자.csv",encoding="EUC-KR")

out = ex.getRecent(df,[],[])
out.name = "전체recent"
out.fillna(0,inplace=True)
out.to_csv("지역별총음식점.csv",encoding="EUC-KR")

flag = False
for i in searchList:
    temp = ex.getRecent(df,i[0],i[1])
    temp.name = i[0][0] + 'recent'
    if flag:
        out = pd.concat([out,temp], axis=1)
    else:
        out = temp.copy()
        flag = True
out.fillna(0,inplace=True)
out.to_csv("지역별최근값.csv",encoding="EUC-KR")

# 데이터 통합
out = pd.read_csv("지역별최근값.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
out1 = pd.read_csv("지역별최근값2.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
out2 = pd.read_csv("지역별최대치.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
out3 = pd.read_csv("지역별총음식점.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
out = pd.concat([out,out1,out2,out3],axis=1)
out.fillna(0, inplace=True)

out2 = pd.read_csv("지역별최대치일자.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
out3 = pd.read_csv("지역별최소치일자.csv",encoding="EUC-KR",index_col=("광역","시군구","읍면동"))
for i in out2.columns:
    out2[i] = pd.to_datetime(out2[i], format ="%Y-%m-%d")
for i in out3.columns:
    out3[i] = pd.to_datetime(out3[i], format ="%Y-%m-%d")
    out[i[:-7] + 'dateRange'] = (out3[i] - out2[i[:-7] + "maxDate"]).apply(lambda x : x.days)
out = pd.concat([out,out2,out3],axis=1)

out2 = pd.read_csv("../../data/법정동인구2023.csv")
out2.rename(columns={'시도명': '광역', '시군구명': '시군구', '읍면동명': '읍면동'}, inplace=True)
out2 = out2.groupby(["광역","시군구","읍면동"]).sum().iloc[:,1:]
out2.drop(columns=["기준연월","리명"],inplace=True)
out2['인구'] = out2[out2.columns[1:]].sum(axis=1)
out2['남자인구'] = out2[out2.columns[out2.columns.str[-2:]=='남자']].sum(axis=1)
out2['여자인구'] = out2[out2.columns[out2.columns.str[-2:]=='여자']].sum(axis=1)
out = pd.concat([out,out2],axis=1)

out.to_csv("전체_임시.csv",encoding='EUC-KR')

