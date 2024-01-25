import os
from threading import Thread
from queue import Queue
import json
import sys
import ast
import re
from dotenv import load_dotenv
load_dotenv()

from models.gemini import Gemini

class GeminiSeed(Gemini):

    def __init__(self, api_key=None, model_name="gemini-pro"):
        super().__init__(api_key, model_name)

        self.INPUT_PROMPT = '''Extract important Hindi keywords and Name from the given sentence. Remember to keep name and keywords seperate. 
All English word and number must be ignored.

sentence: पीटीआई के मुताबिक, यादव ने कहा कि अगले साल लोकसभा चुनाव से पहले तीसरे मोर्चे के गठन की कवायद से विपक्ष की एकजुटता पर असर नहीं पड़ेगा। टीएमसी अध्यक्ष ममता बनर्जी और टीआरएस प्रमुख चंद्रशेखर राव द्वारा तीसरा मोर्चा बनाने की कवायद से विपक्ष की एकता के प्रयासों को धक्का लगने के सवाल पर उन्होंने कहा ‘‘मुझे नहीं लगता कि तीसरा मोर्चा वजूद में आयेगा। कुछ समय इंतजार करें, तीसरा मोर्चा बनाने वाले ही साझा विपक्ष की बात करेंगे। 
Name: ['पीटीआई', 'यादव', 'टीएमसी', 'टीआरएस' ,'ममता बनर्जी', 'चंद्रशेखर राव'], Keywords: [ 'लोकसभा', 'चुनाव', 'पहले',  'तीसरे',  'मोर्चे', 'गठन', 'विपक्ष', 'एकजुटता', 'प्रमुख', 'असर', 'अध्यक्ष', 'मोर्चा', 'एकता', 'प्रयासों', 'धक्का', 'सवाल', 'वजूद' ]
'''

    def process_input(self, data, batch_size=1):

        """
        Process the input data and create a queue for parallel processing.

        Args:
            data (list): List of data items.
            batch_size (int): The batch size for processing.

        Returns:
            Queue: A queue containing input prompts and original rows.
        """
        
        data_queue = Queue()

        for idx in range(0, len(data), batch_size):
            batch = data[idx:idx+batch_size]

            input_prompts = []
            for row in batch:
                text = row["text"][:500] # Taking only the first 500 characters to keep the input prompt short
                input_prompt = self.INPUT_PROMPT + f"\nsentence: {text}"
                input_prompts.append(input_prompt)

            data_queue.put({"input_prompt": input_prompts, "original_rows": batch})

        return data_queue

    def postprocess_response(self, output_rows, original_rows, response_text):

        """
        Postprocess the response and update the original rows with model output.

        Args:
            output_rows (list): List of output rows.
            original_rows (list): List of original rows.
            response_text (str): The raw response text.

        Returns:
            list: Updated original rows with model output.
        """
            
        for idx, row in enumerate(output_rows):
            try:
                Name, Keywords = row.split(", Keywords")

                original_rows[idx]["Name"] = Name.replace("Name:", "").strip()
                original_rows[idx]["Keywords"] = Keywords.replace(":", "").strip()

            except Exception as e:
                print(f"Error parsing output row: {e}")
                original_rows[idx]["Name"] = ""
                original_rows[idx]["Keywords"] = ""

        # Also put original response for safe keeping
        for idx, row in enumerate(original_rows):
            original_rows[idx]["response"] = response_text

        return original_rows
    
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
            row_dict = {"seed": row}
            f.write(json.dumps(row_dict, ensure_ascii=False) + "\n")

def main(input_folder, max_threads=10):

    """
    Main function for processing input files.

    Args:
        input_folder (str): Path to the input folder containing content jsonl files.
    """

    api_key = os.getenv("GEMINI_API_KEY")
    seed_generator = GeminiSeed(api_key=api_key)

    files = os.listdir(input_folder)
    files = [file for file in files if file.endswith(".jsonl")]
    save_folder = input_folder + "_output"

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    for file in files:
        filepath = os.path.join(input_folder, file)
        print(f"Processing file: {filepath}")
        
        output_filepath = os.path.join(save_folder, f"{file.split('.')[0]}_output.jsonl")

        with open(filepath, encoding='utf-8') as f:
            data = [json.loads(line) for line in f]

        data_queue = seed_generator.process_input(data)

        with open(output_filepath, "w", encoding='utf-8') as f:
            threads = [Thread(target=seed_generator.worker, args=(f, data_queue)) for i in range(max_threads)]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

        # Extract out name and keywords. Then drop english words and save.
        data = read_data(output_filepath)
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

        keywords_folder = os.path.join(save_folder, "keywords")
        names_folder = os.path.join(save_folder, "names")

        if not os.path.exists(keywords_folder):
            os.makedirs(keywords_folder)

        if not os.path.exists(names_folder):
            os.makedirs(names_folder)

        save_data(os.path.join(names_folder, f"{file.split('.')[0]}_uq_name.jsonl"), list(uq_name))
        save_data(os.path.join(keywords_folder, f"{file.split('.')[0]}_uq_keywords.jsonl"), list(uq_keywords))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate/seed.py <input_folder>")
        sys.exit(1)

    input_folder = sys.argv[1]
    main(input_folder)