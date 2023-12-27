import json
import ast
import re

def get_list(idx, row, key):

    key_string = row[key]

    key_list = []
    try:
        key_list = ast.literal_eval(key_string)
    except:
        print("Error parsing names list for idx {} row {}".format(idx, row[key]))

    return key_list

def drop_english(data_list):
    # Drop words with english characters or numbers
    pattern = re.compile('[a-zA-Z0-9]')
    data_list = [x for x in data_list if not pattern.match(x) and len(x) > 1]
    return data_list

def read_data(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def save_data(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def main():

    data = read_data('data/output.jsonl')

    uq_name = set()
    uq_keywords = set()

    for idx, row in enumerate(data):

        names_list = get_list(idx, row, 'Name')
        for name in names_list:
            uq_name.add(name)

        keywords_list = get_list(idx, row, 'Keywords')
        for keyword in keywords_list:
            uq_keywords.add(keyword)

    uq_name = drop_english(uq_name)
    uq_keywords = drop_english(uq_keywords)

    save_data('data/uq_name.jsonl', list(uq_name))
    save_data('data/uq_keywords.jsonl', list(uq_keywords))
