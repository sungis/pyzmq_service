#coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pymongo 
from ac_trie import Trie 
import json


tagsParser = Trie('./dict/skill.txt')

conn = pymongo.Connection(host='192.168.4.216', port=19753) 
article = conn.tags.article 
def cut(value):
    value=value.lower().replace('&nbsp','')
    value = value.encode('UTF-8')
    terms = tagsParser.parse(value)
    v = {}
    for i in terms:
        v[i[0]]=i[1]
    return v.values()
    

def load_data(): 
    blog_id_f = open('/home/pongo/gitwork/file_bak/data/blog_id.txt') 
    for id in blog_id_f:
        id=id.strip()
        one = article.find_one({"_id": id}, {"Title": 1,"Description":1,"category":1,'UserName':1})
        text=one["Title"]+one["Description"]+one["category"] 
        url = 'http://blog.csdn.net/'+one["UserName"]+'/article/details/'+str(id)
        terms = cut(text)
        if len(terms)>0:
            print url,'\t',','.join(set(terms))
            break


if __name__ == '__main__':
    load_data()


