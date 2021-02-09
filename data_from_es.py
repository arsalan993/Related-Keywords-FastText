import json
from elasticsearch_dsl import Search, Q
from connections import elasticsearch_connection
from multiprocessing import Pool
from itertools import chain

with open("taxonomies.txt", 'r') as f:
    classes = f.readlines()


def topic_es_search(class_topic):
    search = Search(index='posts-*', using=elasticsearch_connection)
    match_topic = Q('bool', must=[Q("match", topics__label=class_topic)])
    topic_filter = Q("nested", path='topics', query=match_topic)

    lang_filter = Q("term", language="en")
    final_query = Q('bool', must=[lang_filter, topic_filter])
    search = search.query(final_query)
    search = search.source(["title", "short_description", "description"])
    count = search.count()
    print(class_topic, count)
    if count != 0:
        return search
    else:
        return None


def get_data(search):
    list_of_dataset = []
    count = 0

    for item in search.scan():
        # dataset_articles = {}
        if count == 300:
            break
        meta = item.meta.to_dict()
        records = item.to_dict()
        records["id"] = meta.get("id")
        # dataset_articles[meta.get("id")] = records
        list_of_dataset.append(records)
        count += 1
    return list_of_dataset


def get_all_data(topic):
    search_obj = topic_es_search(topic)
    if search_obj is not None:
        return get_data(search_obj)


if __name__ == '__main__':
    returned_dataset = []

    with Pool(60) as p:
        returned_dataset = list(p.map(get_all_data, classes))
    returned_dataset = list(chain(*returned_dataset))
    with open('data.json', 'w') as outfile:
        json.dump(returned_dataset, outfile)
    print("done importing data from ElasticSearch")