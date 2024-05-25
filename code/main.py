import extract as ex

df = ex.init()
searchList = [[['마라'],['중국식','일반조리판매']], [['탕후루'],[]],[['카스테라'],[]],[['떡볶이'],[]],[['포케'],[]],[['양꼬치'],[]],[['곱창'],[]],[['핫도그'],[]],[['타코야끼'],[]],
              [['훠궈'],[]],[['빙수'],[]],[['설빙'],[]],[['요거트'],[]],[['밥버거'],[]],[['베이글'],[]],[['닭발'],[]],[['버블티'],[]],[['공차'],[]],[['닭강정'],[]],
              [['와플'],[]],[['토스트'],[]],[['파스타'],[]],[['샐러드'],[]],[['십원빵'],[]],[['도넛'],[]],[['마카롱'],[]],[['크로플'],[]],[['맥도날드'],[]],[['롯데리아'],[]],
              [['버거킹'],[]],[['서브웨이'],[]],[['케이에프씨'],[]],[['한솥',],[]],[['푸라닭'],[]],[['엽기떡볶이'],[]],[['역전할머니맥주'],[]],[['지코바'],[]]]
temp = searchList[:2]
ex.listToCsv(df, temp)


dflist = []
for i in searchList:
    print(f'Appending {i[0][0]}')
    dflist.append(pd.read_csv(i[0][0]+".csv",index_col=0,encoding="EUC-KR"))

out = pd.DataFrame(index=allRegions, columns=[])
for i in dflist:
    read = i.groupby('지역').max()
    read = read[i.columns[1]]
    read = read.rename(i.columns[1] + 'max')
    out = pd.concat([out,read])
out2 = out.copy()
out2 = out2.fillna(0)
out2['지역']=out2.index
out3=out2.groupby("지역").sum()
out3.to_csv("지역별최대치.csv",encoding="EUC-KR")

out = pd.DataFrame(index=allRegions, columns=[])
for i in dflist:
    read = i.groupby('지역').idxmax(axis=0)[i.columns[1]].str[-10:]
    read = read.rename(i.columns[1] + 'maxDate')
    out = pd.concat([out,read])
    
out4 = out.groupby(out.index).sum()
out4.replace(0, None, inplace=True)
out4.to_csv("지역별최대치일자.csv",encoding="EUC-KR")

out = pd.DataFrame(index=allRegions, columns=[])
for i in dflist:
    read = i[pd.to_datetime(i.연월일)>pd.to_datetime("2004-03-31")]
    read = read.groupby('지역').idxmin(axis=0)[i.columns[1]].str[-10:]
    read = read.rename(i.columns[1] + 'minDate')
    out = pd.concat([out,read])
    
out4 = out.groupby(out.index).sum()
out4.replace(0, None, inplace=True)
out4.to_csv("지역별최소치일자.csv",encoding="EUC-KR")


out = pd.DataFrame(index=allRegions, columns=[])
for i in dflist:
    temp = i.loc[i.groupby('지역').idxmax()["연월일"]]
    temp2 = temp[temp.columns[1]]
    temp2.index = temp.지역
    temp2.rename(temp2.name+'recent',inplace=True)
    out = pd.concat([out,temp2])
out2 = out.copy()
out2 = out2.fillna(0)
out2['지역']=out2.index
out3=out2.groupby("지역").sum()
out3.to_csv("지역별최대치일자.csv",encoding="EUC-KR")

out = ex.csvToOut('마라')

out = pd.DataFrame(index=allRegions, columns=[])
for i in searchList:
    temp = ex.getRecent(df,i[0],i[1])
    temp.name = i[0][0] + 'recent'
    out = pd.concat([out,temp])
    out.fillna(0,inplace=True)
    out = out.groupby(out.index).sum()
out.to_csv("지역별최근값.csv",encoding="EUC-KR")

## 마라 예시
# mala = ex.shopDates(df,['마라'],['중국식','일반조리판매'])
# mala.to_csv("마라예시.csv",encoding='EUC-KR')