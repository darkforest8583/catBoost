

import json
import logging
import os
import jieba
import sys
import operator
from tqdm import tqdm
import pickle
import re
dict_path = os.path.join(os.getenv("JIEBA_DATA"), "dict.txt.big") 
ptt_path = (os.getenv("DATA"))
jieba.set_dictionary(dict_path)
process_files = ['Gossiping', 'Boy-Girl']
marker = {'Gossiping': '>', 'NBA': '<', 'Boy-Girl': '^'}


#count_response = {}

def main():

    Filter = ArticleFilter()

def print2file(f, title, responses, marker = '', separater = True):
    if marker != '':
        f.write(marker + ' ')
    title_cutted = jieba.cut(title.strip(), cut_all=False)
    for word in title_cutted:
        f.write(word + ' ')
    f.write('\n')
    for response in responses:
        #print(response['Content'])
        #if response['Content'] not in count_response.keys():
        #    count_response[response['Content']] = 0
        #count_response[response['Content']] += 1
        if marker != '':
            f.write(marker + ' ')
        response_cutted = jieba.cut(response['Content'].strip(), cut_all=False)
        for word in response_cutted:
            f.write(word + ' ')
        f.write('\n')
    if separater:
        f.write('===\n')

class ArticleFilter(object):

    def __init__(self):

        self.stopwords = None
        self.stoptags = None
        self.raw_data = None
        self.corpus = []
        self.order_response = []
        self.order_titles = []

        self.total_article = 0
        self.article_count = 0

        self.titles = set()
        self.users_info = {}

        self.init_load_stopwords()
        self.url_pattern = '(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9]\.[^\s]{2,})'
        logging.basicConfig(format='%(asctime)s : %(threadName)s : %(levelname)s : %(message)s', level=logging.INFO)

    def init_load_stopwords(self):
        """
        Initialize the stopwords
        """
        with open(os.path.join(ptt_path, 'stopwords/drop_comment.txt'),'r', encoding='utf-8') as sw:
            self.dropwords = [word.strip('\n') for word in sw]
        with open(os.path.join(ptt_path,'stopwords/chinese_sw.txt'), 'r', encoding='utf-8') as sw:
            self.stopwords = [word.strip('\n') for word in sw]
        with open(os.path.join(ptt_path,'stopwords/stopwords-tw.txt'), 'r', encoding='utf-8') as sw:
            self.stopwords += [word.strip('\n') for word in sw]
        with open(os.path.join(ptt_path, 'stopwords/specialMarks.txt'), 'r', encoding='utf-8') as sw:
            self.special_markers = [word.strip('\n') for word in sw]
        with open(os.path.join(ptt_path, 'stopwords/gossiping.tag'),'r', encoding='utf-8') as sw:
            self.stoptags = [word.strip('\n') for word in sw]

    def process_raw_data(self, path, is_dir=False, to_one_file=False, one_file_name="corpus.json", marker=''):

        data = []
        total = []
        filename = None
        count = 0

        if is_dir:
            filenames = [name for name in os.listdir(path) if not name.startswith(".")]
        else:
            filenames = [path]
        
        for filename in filenames:
            count +=1
            if count % 10 == 0:
                logging.info((count, self.article_count, self.total_article))
            with open(os.path.join(path, filename),'r', encoding="utf-8") as data:
                res = self.generate_corpus(json.load(data), marker=marker)

    def generate_corpus(self, articles, drop_response=True, negative_tag=None, no_content=True, min_length=1, marker='', stopwords=False):

        if negative_tag is None:
            negative_tag = self.stoptags

        clean_article = []
        for article in tqdm(articles):

            self.total_article += 1
            try:
                title = article["Title"]
                clean_responses = self.clean_responses(article["Responses"], stopwords=stopwords)
                if len(clean_responses) == 0:
                    continue 
                article["Responses"] = clean_responses
            except Exception as e:
                #print("Wrong Format: %s" % str(e))
                continue
 
            if title in self.titles or len(title) < min_length:

                continue

            if drop_response:

                if title.startswith("Re") or title.startswith("Fw"):
                    continue


            #if no_content:
            #    article.pop("Content")
    
            tag, clean_title = self.get_tag(title) 
            # clean special markers
            for w in self.special_markers:
                clean_title = clean_title.replace(w, ' ')
            article["Tag"]   = tag
            article["Title"] = clean_title

            if tag == '新聞':
                clean_content = self.clean_news(article['Content'])
            else:
                clean_content = self.clean_content(article['Content'], split_line=False)
            article['Raw'] = article['Content']
            article['Content'] = clean_content
            self.titles.add(clean_title)
            self.order_titles.append(clean_title)
            self.order_response.append(clean_responses)

            self.article_count += 1
            clean_article.append(article)
        return clean_article

    def get_url(self, content):
        urls = re.findall(self.url_pattern, content)
        urls = [u.strip('/') for u in urls]
        return urls

    def clean_content(self, content, split_line=True):

        # clean the multiple change line
        content = re.sub('\n+', '\n', content) 
        # clean the url
        content = re.sub(self.url_pattern, '', content)
        # clean the RE pattern
        content = re.sub('引述.*?之銘言', '', content)
        content = re.sub('^:.*?\n ', '', content)
        # clean FB article
        content = re.sub('ＦＢ.*?：', '', content)
        # clean the special marker
        content = re.sub('^※.*?\n', '', content)
        content = re.sub('[:：]', ' ', content)
        # clean html tag
        content = re.sub('<.*?>', '', content)
        content = re.sub('\[.*?\]', '', content)
        content = re.sub('\/.*?\/', '', content)
 
        # clean the non-chinese word (may be useful?)
        #content = re.sub('[”“.,?:\ ][a-z0-9A-Z\ ]+[”“.,?:\ ]', ' ', content)
        # clean the puncatuations
        if split_line:
            content = re.sub('[\ ，。]+', '\n', content)
            content = re.sub('[.、（]+\n', '\n', content)
        else:
            content = re.sub('[\ ，。]+', ' ', content)
            content = re.sub('[.、（]+\ ', ' ', content)

        for w in self.special_markers:
            content = content.replace(w, ' ') 
        return content.strip()

    def clean_news(self, content):

        try:
            source, content = content.split(paragraph_tag[1], 1)
            source = source.split(paragraph_tag[0], 1)[1]
            news_title, content = content.split(paragraph_tag[2], 1)
            news_content, content = content.split(paragraph_tag[3], 1)
            #print('source : ', source.strip())
            #print('title : ', news_title.strip())
            #print('content :', news_content.strip())
            return self.clean_content(news_content, split_line=False)
        except:
            return ''

    def clean_responses(self, responses, negative_user=set(), min_length=5, dropwords=None,stopwords=False):

       

        if dropwords is None:
            dropwords = self.dropwords
        if stopwords:
            stopwords = self.stopwords
        else:
            stopwords = []

        clean_responses = []

        for response in responses:
            #self._update_users_history(response) 
            drop = False


            if response["User"] in negative_user or len(response["Content"]) < min_length:
                drop = True

            for w in stopwords:
                if w in response["Content"]:
                    drop = True
            # Drop the response containing url
            if (len(self.get_url(re.sub('\ +', '//', response["Content"]))) != 0
                or len(self.get_url(response['Content'])) != 0):
                drop = True
            # clean special markers
            for w in self.special_markers:
                response["Content"] = response["Content"].replace(w, ' ')
            response["Content"] = response["Content"].strip()
            if not drop and len(response['Content']) > 0:
                clean_responses.append(response)

        return clean_responses

    def _update_users_history(self, response):


        user = response["User"]

        if user not in self.users_info.keys():

            self.users_info[user] = res


    def get_tag(self, title, debug=False):

        if debug:
            print('Input title:', title)
        try:
            tag,title = title.split("]",1)
            tag = tag.split('[')[1]
        except:

            return None,title

        title = title.lstrip()
        if debug:
            print('Processed tag, title:', tag, title)
        return tag.strip(), title.strip()

    def print_titles(self):


        with open('data/Titles.txt','w',encoding='utf-8') as op:
            for title in self.order_titles:
                op.write(title + "\n")


if __name__ == '__main__':
    main()
