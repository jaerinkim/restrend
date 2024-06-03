import extract as ex
import pandas as pd

df = pd.read_csv("전체.csv",encoding='euc-kr')
slist = ['추어탕', '닭갈비', '오리', '비어', '아구찜', '한우', '삼겹살']
searchList = []
for i in slist:
    searchList.append([[i],[]])
ex.listToCsv(df, searchList)