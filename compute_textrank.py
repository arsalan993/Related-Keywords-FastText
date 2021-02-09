import spacy
from pytextrank import pytextrank
from nltk.tokenize import MWETokenizer, word_tokenize,sent_tokenize
import pandas as pd
import time
from spacy.language import Language
import multiprocessing
from multiprocessing import  Queue,cpu_count,JoinableQueue
import sys
import pickle
from data_clean_pipeline import get_stopwords
from tqdm import tqdm


stop_words_ = get_stopwords()
def convert(list_):
    return (*list_,)

def textrank_compute(queue_input,queue_output):
    while not queue_input.empty():
        text = queue_input.get()
        doc = nlp(text)
        tokens_words = []
        for p in doc._.phrases:
            word = p.text
            word = word.split()
            if 1 <= len(word) < 4:
                tokens_words.append(convert(word))
        mwe = MWETokenizer(tokens_words, separator='_')
        text_to_return = [[word_ for word_ in mwe.tokenize(word_tokenize(sent)) if word_ not in stop_words_] for sent in sent_tokenize(text)]
        # text_to_return = [word_ for word_ in mwe.tokenize(word_tokenize(text)) if word_ not in stop_words_]
        # queue_output.put_nowait(text_to_return)
        for tokenized in text_to_return:
            queue_output.put_nowait(tokenized)
    print("Process completed")
    task_queue.put(True)

def main():
    queue_input = Queue()
    queue_output = JoinableQueue()
    clean_data = pd.read_csv("clean_data.csv")
    corpus =clean_data['refined_text'].to_list()
    for doc in corpus:
        queue_input.put(doc)
    processes = []
    for i in range( cpu_count()):
        p = multiprocessing.Process(target=textrank_compute,args=(queue_input,queue_output),daemon=True)
        processes.append(p)
        p.start()
    result = []
    while True:

        if task_queue.qsize() == cpu_count():
            while 1 :
                print(queue_output.qsize())
                for i in  tqdm(range(queue_output.qsize())):
                    result.append(queue_output.get())
                if queue_output.qsize() == 0:
                    break
            break
        time.sleep(2)
        print("waiting",task_queue.qsize(),cpu_count(),queue_input.qsize(),queue_output.qsize())
    print("return length of sentence ", len(result))
    time.sleep(5)
    with open('sen_tokens.pkl', 'wb') as handle:
        pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print("tokens secured")
    time.sleep(5)
    for proc in processes:
        proc.join()
        time.sleep(1)

    print("we are here exiting")
    return True


if __name__ == '__main__':
    nlp = spacy.load('en_core_web_lg')
    tr = pytextrank.TextRank()
    nlp.add_pipe(tr.PipelineComponent, name='textrank', last=True)
    task_queue = Queue()
    main()