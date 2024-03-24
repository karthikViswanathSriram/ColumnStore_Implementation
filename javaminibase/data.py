import pandas as pd

with open('/Users/poorvikd/Documents/Project/CSE510-JavaMiniBase/javaminibase/sampledata.txt', 'r') as f:
    data = f.readlines()
    db = {'A':[], 'B':[], 'C':[], 'D':[]}
    count = 0
    for line in data[1:]:

        words = line.strip().split("\t")
        db['A'].append(words[0])
        db['B'].append(words[1])
        db['C'].append(int(words[2]))
        db['D'].append(int(words[3]))
        count+=1

        if count == 10001:
            break

df = pd.DataFrame(db)

print(df['C'].value_counts())
        