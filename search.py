import os
from Stemmer import Stemmer
import sys
import time
posting = {}
stemmer = Stemmer('porter')

top_tokens = []
with open('top_tokens_file.txt', 'r') as f:
    top_tokens = f.readlines()
# field_wts = {'b':3 ,'e' :2 ,'i': 6, 't': 10, 'c':2 ,'r' :2 }
output_file  = open('queries_op.txt','w')

def tokenize(word):
    word = word.lower()
    word = stemmer.stemWord(word)
    # print(word)
    return  word

def find_title(doc_id):
    doc_id = int(doc_id)
    title_file = int(doc_id/10000) + 1
    line_num = (doc_id-1)%10000
    fil = 'title/' + str(title_file) + '.txt'
    f = open(fil,'r')
    l = f.readlines()
    return l[line_num] 

def query(mapp,k):
    ranking = {}
    start = time.time()
    for word in mapp:
        fil = find_file(word)
        if fil<1 or fil>len(top_tokens):
            continue
        fil = 'inv_idx/' + str(fil) + '.txt'
        f = open(fil,'r')
        l = f.readline().strip()
        posting = ''
        while l:
            token = l.split()[0]
            if token==word:
                posting = l
                break
            l = f.readline().strip()
        f.close()
        # print(posting)
        if posting=='':
            continue
        posting = posting.split()
        posting.pop(0)
        for fld in posting:
            eql=1
            if fld[0] not in mapp[word]:
                eql = 1/100
            fld = fld.split('-')[1]
            fld = fld.split(',')
            fld.pop()
            for doc in fld:
                doc_id = doc.split(':')[0]
                tf_idf = float(doc.split(':')[1])*eql
                if doc_id in ranking:
                    ranking[doc_id]+=tf_idf
                else:
                    ranking[doc_id]=tf_idf
    
    ranking = sorted(ranking.items(),reverse=True, key = lambda kv:(kv[1], kv[0]))
    res = k
    for doc in ranking:
        nm = find_title(doc[0])
        print(doc[0],nm)
        output_file.write(doc[0] + ', ' + nm)
        k-=1
        if(k==0):
            output_file.write(str(time.time()-start) + ', ' +  str((time.time()-start)/res) + '\n\n')
            break
    if(k!=0):
        output_file.write(str(time.time()-start) + ', ' +  str((time.time()-start)/res) + '\n\n')


def find_file(word):
    l = 0
    r = len(top_tokens)-1
    while l<=r:
        m = int((l+r)/2)
        if word >= top_tokens[m]:
            l = m+1
        else:
            r = m-1
    return l


if __name__ == "__main__":
    # while True:
        # print('Enter your Query')
        # inp = input()
        # print(inp)
        qfile = sys.argv[1]
        f = open(qfile,'r')
        queries = f.readlines()
        for inp in queries:
            results = int(inp.split(',',1)[0])
            inp = inp.split(',',1)[1]
            inp = inp.strip()
            if ':' in inp:
                mapp  = {}
                while True:
                    idx = inp.find(':')
                    if idx==-1:
                        break
                    field = inp[idx-1]
                    # print(field)
                    inp = inp[idx+1:]
                    idx = inp.find(":")
                    if idx!=-1:
                        quer = inp[:idx-1]
                        quer = quer.split()
                        for word in quer:
                            word = tokenize(word)
                            if word  in mapp:
                                mapp[word].append(field)
                            else:
                                mapp[word] = [field]
                        inp = inp[idx-1:]
                    else:
                        inp = inp.split()
                        for word in inp:
                            word = tokenize(word)
                            if word in mapp:
                                mapp[word].append(field)
                            else:
                                mapp[word] = [field]
                        break
                query(mapp,results)
            else:
                mapp = {}
                inp = inp.split()
                for word in inp:
                    word = tokenize(word)
                    mapp[word] = ['b','t']
                query(mapp,results)
        output_file.close()