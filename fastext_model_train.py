from gensim.models import FastText
import pickle

with open('sen_tokens.pkl', 'rb') as handle:
    token_text = pickle.load(handle)
print(len(token_text))

"""sg=1, hs=0, size=100, window=8, min_count=150,
                 word_ngrams=1, workers=64,
                 iter=5, min_n=3, max_n=7, negative=20"""
model = FastText(sg=1, hs=1, size=150, window=4, min_count=100,
                 word_ngrams=1, workers=64,
                 iter=5, min_n=3, max_n=7, negative=30)  # instantiate
model.build_vocab(sentences=token_text)
model.train(sentences=token_text, total_examples=len(token_text), epochs=10, threads=55)
model.save("model/fastext_model")
