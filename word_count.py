#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext
from operator import add
import nltk
nltk.download('punkt')
nltk.download('stopwords')
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
import string
from HTMLParser import HTMLParser
import sys
import re

# print sys.argv[2]

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return u''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

conf = SparkConf()
conf.setAppName("Word Count")
conf.set("spark.executor.instances", "15")
conf.set("spark.executor.memory", "10g")
conf.set("spark.executor.cores", "3")
sc = SparkContext(conf = conf)
sc.setLogLevel('INFO')


stopWords = stopwords.words('english')
stopWords += stopwords.words('french')
stopWords += stopwords.words('german')
stopWords += stopwords.words('italian')
otherWords = [u'limited', u'all', u'whoever', u'dollar', u'month', u'four', u'legal', u'go', u'mill', u'disk', u'contributions', u'yonder', u'seemed', u'whose', u'hold', u'ver', u'calculate', u'wyoming', u'including', u'literary', u'to', u'tm', u'present', u'solicit', u'under', u'arkansas', u'include', u'sent', u'case', u'very', u'pages', u'liability', u'every', u'yourselves', u'exact', u'method', u'arrange', u'cause', u'list', u'hawaii', u'small', u'indemnity', u'disclaimer', u'possibility', u'the', u'ten', u'requirements', u'discover', u'licensed', u'wherefore', u'titles', u'kentucky', u'pmb', u'download', u'further', u'estimated', u'even', u'what', u'errors', u'selected', u'maine', u'version', u'above', u'new', u'net', u'increasing', u'ever', u'public', u'thin', u'edited', u'full', u'hart', u'never', u'commercial', u'here', u'hours', u'met', u'let', u'pub', u'others', u'alone', u'along', u'contributing', u'fifteen', u'appears', u'alabama', u'sending', u'search', u'property', u'disclaimers', u'oxford', u'items', u'compliance', u'wherever', u'alaska', u'leaving', u'amount', u'periodic', u'products', u'suggestion', u'usually', u'honor', u'named', u'via', u'secure', u'vii', u'prefer', u'ask', u'send', u'apply', u'txt', u'estimate', u'market', u'readily', u'yourself', u'permitted', u'fee', u'from', u'takes', u'would', u'stated', u'readable', u'two', u'next', u'equipment', u'few', u'program', u'call', u'therefore', u'indiana', u'themselves', u'thru', u'until', u'more', u'paperwork', u'clearly', u'becomes', u'hereby', u'corrected', u'limitation', u'award', u'herein', u'connecticut', u'everywhere', u'particular', u'expenses', u'iii', u'must', u'me', u'organizations', u'none', u'word', u'hour', u'rights', u'this', u'work', u'expends', u'ere', u'anywhere', u'nine', u'can', u'ms', u'v', u'following', u'my', u'dedicated', u'proprietary', u'processing', u'give', u'december', u'approved', u'newsletters', u'tax', u'accept', u'days', u'states', u'professor', u'something', u'want', u'arise', u'information', u'united', u'end', u'goal', u'defective', u'rather', u'rhode', u'plans', u'six', u'damage', u'hampshire', u'machine', u'how', u'donors', u'answer', u'product', u'express', u'intended', u'may', u'after', u'damaged', u'corrections', u'hereupon', u'produce', u'prohibition', u'date', u'such', u'law', u'data', u'efforts', u'processors', u'a', u'owns', u'physical', u'third', u'whenever', u'columbia', u'st', u'provide', u'ones', u'so', u'allow', u'pay', u'volunteers', u'order', u'refund', u'help', u'indeed', u'over', u'move', u'midnight', u'years', u'xi', u'through', u'fify', u'still', u'its', u'before', u'derive', u'exclusion', u'thence', u'somewhere', u'winning', u'better', u'harmless', u'production', u'copies', u'listing', u'consequential', u'ours', u'freely', u'might', u'alter', u'texas', u'instantly', u'then', u'them', u'someone', u'iowa', u'thereby', u'docs', u'thee', u'ye', u'number', u'they', u'half', u'not', u'now', u'day', u'nor', u'february', u'name', u'materials', u'always', u'university', u'whither', u'good', u'breach', u'each', u'found', u'ebooks', u'side', u'trailer', u'domain', u'everyone', u'cost', u'directly', u'michael', u'eg', u'fees', u'related', u'applicable', u'books', u'punitive', u'taxes', u'year', u'our', u'beyond', u'additions', u'special', u'out', u'ftp', u'oklahoma', u'since', u'lawyers', u'forty', u're', u'millennium', u'reader', u'print', u'got', u'receiving', u'forth', u'thereupon', u'foundation', u'wisconsin', u'written', u'million', u'free', u'whereupon', u'york', u'besides', u'ohio', u'put', u'org', u'anyhow', u'software', u'advance', u'hardware', u'service', u'created', u'could', u'florida', u'xii', u'keep', u'thine', u'ltd', u'hence', u'merchantability', u'onto', u'south', u'first', u'already', u'seeming', u'revenue', u'feel', u'thereafter', u'wast', u'one', u'done', u'defects', u'another', u'twas', u'message', u'given', u'publication', u'district', u'indicate', u'defect', u'twenty', u'top', u'trillion', u'system', u'least', u'anyone', u'their', u'too', u'statement', u'legally', u'hundred', u'final', u'listed', u'mostly', u'that', u'nobody', u'part', u'july', u'copy', u'herself', u'than', u'population', u'kind', u'aught', u'see', u'project', u'nevertheless', u'future', u'oregon', u'were', u'toward', u'displays', u'and', u'beforehand', u'mine', u'return', u'licenses', u'medium', u'have', u'need', u'seem', u'any', u'contents', u'conversion', u'check', u'transcription', u'latter', u'nebraska', u'maximum', u'indexes', u'note', u'also', u'contact', u'take', u'which', u'online', u'registered', u'begin', u'towards', u'unless', u'though', u'who', u'reach', u'sue', u'paid', u'most', u'donation', u'montana', u'eight', u'amongst', u'letter', u'preliminary', u'nothing', u'accepting', u'underline', u'why', u'don', u'noone', u'sometimes', u'request', u'amoungst', u'exclusions', u'prepare', u'infringement', u'responded', u'incomplete', u'pobox', u'show', u'text', u'ascii', u'anyway', u'fifty', u'nevada', u'find', u'staff', u'nominally', u'editing', u'based', u'copyright', u'distributed', u'behind', u'should', u'being', u'only', u'employee', u'identification', u'posted', u'hypertext', u'carolina', u'do', u'his', u'handle', u'goes', u'get', u'displayed', u'michigan', u'de', u'etext', u'cannot', u'hie', u'international', u'during', u'him', u'is', u'reading', u'sources', u'cry', u'alteration', u'remove', u'email', u'book', u'paper', u'she', u'contain', u'release', u'x', u'finishing', u'where', u'scanning', u'compressed', u'set', u'art', u'tilde', u'notice', u'up', u'edition', u'namely', u'computer', u'are', u'corrupt', u'encouraged', u'instance', u'wire', u'pennsylvania', u'ways', u'away', u'please', u'sites', u'filename', u'enough', u'converted', u'various', u'electronically', u'between', u'progress', u'neither', u'donate', u'approach', u'across', u'we', u'august', u'web', u'modify', u'however', u'gratefully', u'extent', u'analyzed', u'both', u'dates', u'proofread', u'last', u'license', u'thou', u'many', u'whereafter', u'against', u'etc', u's', u'became', u'expense', u'whole', u'jersey', u'otherwise', u'comment', u'among', u'co', u'afterwards', u'seems', u'whatever', u'hast', u'tennessee', u'agents', u'moreover', u'projected', u'throughout', u'november', u'described', u'second', u'addition', u'west', u'create', u'trademark', u'three', u'been', u'mark', u'whom', u'much', u'pg', u'interest', u'entered', u'louisiana', u'empty', u'choose', u'remedies', u'received', u'fire', u'latterly', u'thousand', u'c', u'else', u'fund', u'understand', u'gross', u'despite', u'hers', u'former', u'those', u'paying', u'myself', u'georgia', u'resulting', u'these', u'plain', u'means', u'bill', u'missouri', u'value', u'will', u'receive', u'while', u'xiii', u'viii', u'error', u'deductible', u'ein', u'ix', u'annual', u'mexico', u'almost', u'vi', u'sincere', u'thus', u'it', u'iv', u'ii', u'cant', u'itself', u'contract', u'in', u'ave', u'ie', u'if', u'binary', u'confirmed', u'inc', u'author', u'perhaps', u'things', u'make', u'punctuation', u'header', u'disclaims', u'same', u'wherein', u'beside', u'rules', u'html', u'damages', u'modification', u'party', u'several', u'status', u'used', u'http', u'again', u'promo', u'island', u'upon', u'solicited', u'codes', u'purpose', u'characters', u'interested', u'opportunity', u'vermont', u'off', u'whereby', u'thy', u'i', u'com', u'well', u'obviously', u'person', u'without', u'does', u'oft', u'organization', u'meanwhile', u'con', u'yours', u'negligence', u'just', u'less', u'yea', u'money', u'distribute', u'front', u'identify', u'questions', u'using', u'dakota', u'october', u'yet', u'dost', u'announcement', u'readers', u'theect', u'had', u'except', u'cataloguers', u'internal', u'association', u'has', u'around', u'provisions', u'alternatively', u'read', u'utah', u'virginia', u'briefest', u'royalty', u'five', u'know', u'world', u'accepted', u'hereafter', u'like', u'subscribe', u'follows', u'incidental', u'audience', u'donations', u'either', u'january', u'become', u'fault', u'page', u'replacement', u'therein', u'twelve', u'www', u'because', u'often', u'people', u'tells', u'some', u'back', u'calculated', u'added', u'convey', u'indirect', u'newsletter', u'ourselves', u'provided', u'total', u'for', u'bottom', u'delaware', u'per', u'conservative', u'wrong', u'everything', u'trustees', u'prof', u'warranties', u't', u'be', u'ebook', u'inaccurate', u'equivalent', u'fitness', u'use', u'nowhere', u'although', u'sixty', u'by', u'on', u'about', u'works', u'central', u'working', u'anything', u'of', u'dollars', u'accepts', u'virus', u'costs', u'whence', u'texts', u'permission', u'or', u'printed', u'warranty', u'own', u'letters', u'formerly', u'into', u'within', u'washington', u'intellectual', u'due', u'down', u'transcribe', u'references', u'couldnt', u'your', u'mississippi', u'additional', u'her', u'eleven', u'transfer', u'there', u'long', u'payment', u'start', u'ibiblio', u'way', u'forward', u'editions', u'was', u'strict', u'files', u'royalties', u'himself', u'north', u'elsewhere', u'form', u'offer', u'constantly', u'becoming', u'but', u'somehow', u'payable', u'state', u'explanatory', u'filed', u'trying', u'with', u'considerable', u'raising', u'he', u'made', u'portions', u'versions', u'whether', u'wish', u'official', u'thickv', u'us', u'tell', u'ebcdic', u'record', u'below', u'un', u'distribution', u'agree', u'delete', u'associated', u'links', u'describe', u'am', u'sales', u'kansas', u'an', u'as', u'sometime', u'right', u'at', u'file', u'ay', u'archive', u'fill', u'yon', u'hasnt', u'no', u'whereas', u'when', u'detail', u'tis', u'til', u'other', u'details', u'you', u'nay', u'indemnify', u'users', u'separate', u'searched', u'asterisk', u'illinois', u'gutenberg', u'massachusetts', u'included', u'required', u'together', u'original', u'implied', u'reprinted', u'time', u'serious', u'requires', u'indirectly', u'profits', u'once']
stopWords += otherWords
stopWords = set(stopWords)

porter = PorterStemmer()


# text = sc.textFile("file:/home/parallels/Desktop/hw2/hw2_samples/*")
text = sc.textFile(sys.argv[1], 1000)
text = text.map(lambda contents: re.sub("[^a-zA-Z ]+", ' ',strip_tags(contents).lower()))
# text = text.map(lambda contents: re.sub("[']+", '', contents))
text = text.flatMap(lambda x: x.split(' '))
text = text.filter(lambda x: x not in stopWords)
text = text.map(lambda x: porter.stem(x))
text = text.filter(lambda x: x.isalpha() and len(x) > 2)
text = text.map( lambda w: (w,1))
text = text.reduceByKey(add, 1000)
text = text.sortBy(lambda x: x[1], ascending=False)
top_words = text.top(1000, key=lambda x: x[1]) 
top_words = sc.parallelize([x[0] for x in top_words])


top_words.coalesce(1).saveAsTextFile(sys.argv[2])
