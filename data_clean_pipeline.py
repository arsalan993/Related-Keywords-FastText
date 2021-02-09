import sys
import pandas as pd
import html2text
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from urlextract import URLExtract
import re
from cleantext import clean
import string
import swifter

extractor = URLExtract()


def html_to_text(text):
    output = None
    if text:
        try:
            h = html2text.HTML2Text()
            h.ignore_images = True
            h.ignore_links = True
            output = BeautifulSoup(text,features="html.parser")
            output = h.handle(output.get_text())
            output = output.strip()
            output = output.encode("ascii", "ignore")
            output = output.decode()
        except:
            output = str(output)
    return output


def process_word(word):
    word = re.sub("[^’'.,A-Za-z0-9]+", "", word)
    word = word.replace(",", " , ").replace(".", " . ")
    return word


def clean_text(text):
    # remove hyperlinks
    if text:
        try:
            text = str(text)
            urls = extractor.find_urls(text)
            for url in urls:
                text = text.replace(url, '')

            # removing emails
            emails = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", text)
            for email in emails:
                text = text.replace(email, '')

            # remove urls
            text = " ".join(process_word(item) for item in text.split())
            text = clean(text, replace_with_url="", lower=False)

            # remove old style retweet text "RT"
            text = re.sub(r'^RT[\s]+', '', text)

            # dictionary consisting of the contraction and the actual value
            Apos_dict = {"'s": " is", "n't": " not", "'m": " am", "'ll": " will",
                         "'d": " would", "'ve": " have", "'re": " are", "n\x92t": "not"}

            # replace the contractions
            for key, value in Apos_dict.items():
                if key in text:
                    text = text.replace(key, value)

            # remove punctuation
            string.punctuation = string.punctuation.replace(".", "")
            string.punctuation = string.punctuation.replace(",", "")
            text = "".join([w for w in text if w not in string.punctuation])
        except:
            text = str(text)
    return text


def get_stopwords():
    stop_words = set(stopwords.words('english'))
    return stop_words


def apply_stopwords(text):
    text = ' '.join([word for word in text.split() if word not in cachedStopWords])
    return text


def comb_title_desc(row):
    title = ''
    description = ''
    short_desc = ''

    title = row['title']
    description = row['description']
    short_desc = row['short_description']

    if description is not None or description != '':
        return '{}. {}'.format(title, description)
    elif description is None or description == '':
        return '{}. {}'.format(title, short_desc)


if __name__ == '__main__':
    cachedStopWords = get_stopwords()
    dataframe = pd.read_json("data.json")
    print("dataset size : ",dataframe.shape)
    # dataframe = dataframe.head(50)
    dataframe['description'] = dataframe['description'].swifter.allow_dask_on_strings(enable=True).apply(html_to_text)
    dataframe['short_description'] = dataframe['short_description'].swifter.allow_dask_on_strings(enable=True).apply(html_to_text)
    dataframe['title'] = dataframe['title'].swifter.allow_dask_on_strings(enable=True).apply(html_to_text)
    dataframe['description'] = dataframe['description'].swifter.allow_dask_on_strings(enable=True).apply(clean_text)
    dataframe['short_description'] = dataframe['short_description'].swifter.allow_dask_on_strings(enable=True).apply(clean_text)
    dataframe['title'] = dataframe['title'].swifter.allow_dask_on_strings(enable=True).apply(clean_text)
    dataframe.dropna(axis=0, how="all", subset=['description', 'short_description', 'title'], inplace=True)
    dataframe['refined_text'] = dataframe.swifter.allow_dask_on_strings(enable=True).apply(comb_title_desc, axis=1)
    dataframe.dropna(axis=0, how="all", subset=['refined_text'], inplace=True)
    dataframe.to_csv("clean_data.csv",columns=['refined_text'])
    print("done cleaning data")
