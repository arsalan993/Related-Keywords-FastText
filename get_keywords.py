from gensim.models import FastText

model = FastText.load("model/fastext_model")
while True:
    name = input("Please enter a keyword : ")
    results = model.wv.most_similar(name,topn=50)
    for i in results:
        print(i)
