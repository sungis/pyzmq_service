from gensim import corpora, models, similarities
from ac_trie import Trie
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

tagsParser = Trie('dict/skill.txt')


def cut(value):
    value=value.lower().replace('&nbsp','')
    
    terms = tagsParser.parse(value)
    v = {}
    for i in terms:
        v[i[0]]=i[1]
    return v.values()

f=open('/home/pongo/gitwork/ansj_fast_lda/test_data/sample2.txt')
texts =[]
n = 10
for t in f:
     t = t.strip()
     if len(t)>0:
          words=cut(t)   
          texts.append(words)


dictionary = corpora.Dictionary(texts)
corpus = [dictionary.doc2bow(text) for text in texts]
tfidf = models.TfidfModel(corpus)
corpus_tfidf = tfidf[corpus]
lda = models.LdaModel(corpus_tfidf, id2word=dictionary, num_topics=n)
k = 20
for i in range(n):     
     t = lda.show_topic(i,k)
     print 'topic',i,':'
     for j in t:
         print '\t',j[1],j[0]



