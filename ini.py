import xml.sax
from datetime import datetime
import os
from spacy.lang.en.stop_words import STOP_WORDS

from Stemmer import Stemmer
import sys
import re
from collections import Counter
from multiprocessing import Process, Queue
import functools
import math
print(datetime.now().time())
stemmer = Stemmer('porter')
pqueue = Queue()
chunk_size = 10000
index_path = ''
def tokenize(text):
    text = re.sub(r'[^a-z0-9 ]',' ',text)
    text = text.split()
    # text = [x for x in text if x not in STOP_WORDS and len(x)>2]
    fil = []
    for x in text:
        if x in STOP_WORDS or len(x)<=1 or len(x)>16:
            continue
        if len(x)>4:
            try:
                int(x)
                continue
            except:
                pass
        fil.append(x)
    # retur n fil
    text = [stemmer.stemWord(x) for x in fil]
    fil.clear()
    return text

def category(text):
    lists = re.findall(r"\[\[Category:(.*)\]\]", str(text))
    ans = []
    for curr in lists:
        temp = tokenize(curr)
        ans += temp
    return ans

def external_links(text): 
    raw = text.split("==external links==")
    links = ""
    if len(raw) <= 1:
        return []
    raw = raw[1].split("\n")
    for line in raw:
        if line and line[0] == '*':
            links += line + ' '
        else:
            break
    return tokenize(links)     


def references(text):
    text = text.split('==references==')
    if(len(text)<=1):
        return []
    text = text[1]
    text = text.split('\n')
    refs = ""
    for line in text:
        try:
            if line and ((line[0]=='{' and line[1]=='{') or line[0]=='*'):
                # print('k')
                if 'defaultsort' in line:
                    break
                if 'reflist' not in line:
                    refs += line + " "
        except:
            pass
    return tokenize(refs)
    

def infobox(text):
    text = text.split('{{infobox')
    if(len(text)<=1):
        return []
    text  = text[1]
    text  = text.split('\n')
    info=''
    k=1
    for line in text:
        if(line=='}}') or k>20:
            break
        line = line.split('=')
        if(len(line)>1):
            k+=1
            info += line[1] + ' '
    # print(k)
    return tokenize(info)


def index_docs(queue, index_path, title_path):
    inv_idx = {}
    titles = []
    cnt=0
    while True:
        data = queue.get()
        if(data=='Done'):
            print('Done', cnt, len(titles))
            filename = os.path.join(index_path, str(cnt)+'.txt')
            title_file = os.path.join(title_path, str(cnt) + '.txt')
            cnt+=1
            store_in_file(filename, inv_idx, title_file, titles)
            inv_idx = {}
            titles = []
        elif(data=='End'):
            print('before merging', cnt)
            filename = os.path.join(index_path, str(cnt)+'.txt')
            title_file = os.path.join(title_path, str(cnt) + '.txt')
            store_in_file(filename, inv_idx, title_file, titles)
            inv_idx = {}
            titles = []
            break
        else:
            titles.append(data[2])
            for f in data[1]:
                c = Counter()
                for w in data[1][f]:
                    c[w]+=1
                for w in c:
                    if w in inv_idx:
                        if f in inv_idx[w]:
                            inv_idx[w][f].append((data[0], c[w]))
                        else:
                            inv_idx[w][f] = [(data[0], c[w])]
                    else:
                        inv_idx[w] = {}
                        inv_idx[w][f] = [(data[0], c[w])]

    # queue.put(len(inv_idx))
    # store_in_file(filename,inv_idx)
    return
    


def store_in_file(filename, inv_idx, title_file, titles):
    print('storing in file')
    print(datetime.now().time())
    with open(filename, 'w') as f:
        tokens = sorted(inv_idx.keys())
        for token in tokens:
            f.write(token)
            f.write(" ")
            field_keys = sorted(inv_idx[token].keys())
            for field in field_keys:
                f.write(field)
                f.write("-")
                for doc_id, cnt in inv_idx[token][field]:
                    f.write(str(doc_id))
                    f.write(":")
                    f.write(str(cnt)+',')
                f.write(' ')
            f.write("\n")
    with open(title_file, 'w')  as f:
        for l in titles:
            f.write(l + '\n')

class WikiContentHandler(xml.sax.ContentHandler):
    
    def __init__(self):
        self.data = ""
        self.tokens = 0
        self.doc_id = 1
        self.doc_cnt=1
        self.titles = []
        self.inv_idx = {}

    def startElement(self, tag, attributes):
        if tag == 'page':
            self.page = {}
        self.data = ""

    def endElement(self, tag):
        if tag == 'page':
            title = self.page['t']
            self.page['t'] = tokenize(self.page['t'].lower())
            self.page['b'] = self.page['b'].lower()
            qwe = self.page['b'].split()
            self.tokens+=len(qwe)
            self.page['c'] = category(self.page['b'])
            self.page['e'] = external_links(self.page['b'])
            self.page['r'] = references(self.page['b'])
            self.page['i'] = infobox(self.page['b'])
            self.page['b'] = tokenize(self.page['b'])
            self.titles.append(title)
            for f in self.page:
                c = Counter()
                for w in self.page[f]:
                    c[w]+=1
                for w in c:
                    if w in self.inv_idx:
                        if f in self.inv_idx[w]:
                            self.inv_idx[w][f].append((self.doc_id, c[w]))
                        else:
                            self.inv_idx[w][f] = [(self.doc_id, c[w])]
                    else:
                        self.inv_idx[w] = {}
                        self.inv_idx[w][f] = [(self.doc_id, c[w])]
            # pqueue.put((self.doc_id,self.page,title))
            if(self.doc_id%chunk_size==0):
                # pqueue.put('Done')
                filename = os.path.join(index_path, str(self.doc_cnt)+'.txt')
                title_file = os.path.join('title', str(self.doc_cnt) + '.txt')
                store_in_file(filename, self.inv_idx, title_file, self.titles)
                self.titles.clear()
                self.inv_idx.clear()
                self.doc_cnt+=1
            self.doc_id += 1
            self.page = {}
            self.data = ""
            # self.cnt+=1

        elif tag == 'text':
            self.page['b'] = self.data
            self.data = ""

        elif tag == 'title':
            self.page['t'] = self.data
            self.data = ""

    def endDocument(self):
        filename = os.path.join(index_path, str(self.doc_cnt)+'.txt')
        title_file = os.path.join('title', str(self.doc_cnt) + '.txt')
        store_in_file(filename, self.inv_idx, title_file, self.titles)
        self.titles.clear()
        self.inv_idx.clear()
        # pqueue.put('End')

    def characters(self, content):
        self.data += content
        

def merge_2_lines(l1,l2):
    l1 = l1.split()
    l2 = l2.split()
    word = l1[0]
    try:
        l1 = l1[1:]
        l2 = l2[1:]
    except:
        pass
    l3 = ''
    while l1 and l2:
        if(l1[0][0]<l2[0][0]):
            l3 = l3 + ' ' + l1[0]
            l1.pop(0)
        elif(l2[0][0]<l1[0][0]):
            l3 = l3 + ' ' + l2[0]
            l2.pop(0)
        else:
            doc1 = int(l1[0].split('-')[1].split(":")[0])
            doc2 = int(l2[0].split('-')[1].split(":")[0])
            if doc1<doc2:
                l3 = l3 + ' ' + l1[0] + l2[0][2:]
            else:
                l3 = l3 + ' ' + l2[0] + l1[0][2:]
            l1.pop(0)
            l2.pop(0)
    while l1:
        l3 = l3 + ' ' + l1[0]
        l1.pop(0)
    while l2:
        l3 = l3 + ' ' + l2[0]
        l2.pop(0)
    return word + l3



def merge_2_files(f1,f2,index_path):
    new_fl = os.path.join(index_path, 'tmp.txt')
    with open(f1,'r') as f1r:
        with open(f2,'r') as f2r:
            with open(new_fl,'w+') as f3:
                l1 = f1r.readline().strip()
                l2 = f2r.readline().strip()
                while l1 and l2:
                    w1 = l1.split(' ',1)[0]
                    w2 = l2.split(' ',1)[0]
                    if(w1<w2):
                        f3.write(l1 + '\n')
                        l1 = f1r.readline().strip()
                    elif(w2<w1):
                        f3.write(l2 + '\n')
                        l2 = f2r.readline().strip()
                    else:
                        f3.write(merge_2_lines(l1,l2) + '\n')
                        l1 = f1r.readline().strip()
                        l2 = f2r.readline().strip()
                while l1:
                    f3.write(l1 + '\n')
                    l1 = f1r.readline().strip()
                while l2:
                    f3.write(l2 + '\n')
                    l2 = f2r.readline().strip()
    os.remove(f1)
    os.remove(f2)
    os.rename(new_fl, f1)
    return




def merge_files(index_path):
    files = os.listdir(index_path)
    while len(files)>1:
        file1 = os.path.join(index_path, files[0])
        file2 = os.path.join(index_path, files[1])
        merge_2_files(file1,file2,index_path)
        files.append(files[0])
        files = files[2:]
    temp_file = os.path.join(index_path, 'tmp.txt')
    fil = os.path.join(index_path, files[0])
    os.rename(fil,temp_file)
    return temp_file

field_wts = {'b':2 ,'e' :2 ,'i': 6, 't': 12, 'c':2 ,'r' :2 }

def calc_tf_idf(line, num_pages):
    line = line.split()
    token = line[0]
    line = line[1:]
    new_ln = ''
    for fd in line:
        fld = fd.split('-')[0]
        lst = fd.split('-')[1].split(',')
        num_d = len(lst)
        new_ln += ' ' + fld + '-'
        for doc in lst:
            if doc:
                cnt = int(doc.split(":")[1])
                doc_id = int(doc.split(':')[0])
                tf = math.log(cnt) + 1
                idf = math.log(num_pages/num_d) + 1
                tf_idf  = tf*idf*field_wts[fld]
                new_ln+= str(doc_id) + ':' + str(int(tf_idf)) + ','
    return token + new_ln 


def split_files(index_path, temp_file, num_pages,  top_file):
    f_counter=1
    l_counter=0
    total_tok=0
    zxc = open(top_file, 'w')
    with open(temp_file,'r') as f:
        l = f.readline().strip()
        while l:
            total_tok+=1
            if l_counter%chunk_size==0:
                l_counter=0
                zxc.write(l.split()[0] + '\n')
                f2 = os.path.join(index_path, str(f_counter) + '.txt')
                f_counter+=1
                f2w = open(f2,'w+')
            new_l = calc_tf_idf(l,num_pages)
            f2w.write(new_l + '\n')
            # f2w.write(l + '\n')
            l = f.readline().strip()
            l_counter+=1
            if l_counter%chunk_size==0:
                f2w.close()
    print('Total tokens in index = ', total_tok)
    os.remove(temp_file)
            

def main(dump, index_pathh):
    global index_path
    if not os.path.exists(index_pathh):
        os.makedirs(index_pathh)
    if not os.path.exists('title'):
        os.makedirs('title')
    index_path = index_pathh
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    Handler = WikiContentHandler()
    parser.setContentHandler(Handler)
    parser.parse(dump)
    temp_file = merge_files(index_pathh)
    num_pages = Handler.doc_id
    split_files(index_pathh, temp_file, num_pages, 'top_tokens_file.txt')
    print(datetime.now().time())
    # print('total tokens = ',Handler.tokens)
    print("number of docs = ", Handler.doc_id)
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])