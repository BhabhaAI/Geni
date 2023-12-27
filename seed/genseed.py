import sys
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from threading import Thread
from queue import Queue
import os
import json
import time

import google.generativeai as genai

# Load environment variables
load_dotenv()

# Constants
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MAX_THREADS = 10
RATE_LIMIT = 45  # requests per minute
INPUT_PROMPT = '''Extract important Hindi keywords and Name from the given sentence. Remember to keep name and keywords seperate. 
All English word and number must be ignored.

sentence: पीटीआई के मुताबिक, यादव ने कहा कि अगले साल लोकसभा चुनाव से पहले तीसरे मोर्चे के गठन की कवायद से विपक्ष की एकजुटता पर असर नहीं पड़ेगा। टीएमसी अध्यक्ष ममता बनर्जी और टीआरएस प्रमुख चंद्रशेखर राव द्वारा तीसरा मोर्चा बनाने की कवायद से विपक्ष की एकता के प्रयासों को धक्का लगने के सवाल पर उन्होंने कहा ‘‘मुझे नहीं लगता कि तीसरा मोर्चा वजूद में आयेगा। कुछ समय इंतजार करें, तीसरा मोर्चा बनाने वाले ही साझा विपक्ष की बात करेंगे। 
Name: ['पीटीआई', 'यादव', 'टीएमसी', 'टीआरएस' ,'ममता बनर्जी', 'चंद्रशेखर राव'], Keywords: [ 'लोकसभा', 'चुनाव', 'पहले',  'तीसरे',  'मोर्चे', 'गठन', 'विपक्ष', 'एकजुटता', 'प्रमुख', 'असर', 'अध्यक्ष', 'मोर्चा', 'एकता', 'प्रयासों', 'धक्का', 'सवाल', 'वजूद' ]
'''

# Configure generative AI
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=60)
def call_api(data_item, retry_count=0, max_retries=3):

    """
    Call the generative AI API with rate limiting and retry logic.

    Args:
        data_item (str): The input prompt for the API.
        retry_count (int): The current retry count.
        max_retries (int): The maximum number of retries.

    Returns:
        str: The API response.
    """

    if retry_count < max_retries:
        try:
            response = model.generate_content(data_item, generation_config=genai.types.GenerationConfig(
                max_output_tokens=3000,
                temperature=0.0),
                safety_settings={'HARASSMENT':'block_none', 'HATE_SPEECH': 'block_none', 'DANGEROUS': 'block_none', 'SEXUAL': 'block_none'})
            return response
        except Exception as e:

            retry_count += 1

            if "429" in str(e): 
                print("429 error: Rate limit exceeded. Sleeping for 80 seconds.")
                time.sleep(80)
                return call_api(data_item, retry_count, max_retries)  # Retry the same request after sleeping  

            if "500" in str(e):
                print("500 error: Internal server error. Sleeping for 80 seconds.")
                time.sleep(80)
                return call_api(data_item, retry_count, max_retries)  # Retry the same request after sleeping
            
            print(f"API call failed: {e}, retrying... Attempt {retry_count} of {max_retries}")
            return call_api(data_item, retry_count, max_retries)        
    else:
        print("Maximum retries reached. Moving on to the next item.")
        return None
    
def process_input(data, batch_size=1):

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
            input_prompt = INPUT_PROMPT + f"\nsentence: {text}"
            input_prompts.append(input_prompt)

        data_queue.put({"input_prompt": input_prompts, "original_rows": batch})

    return data_queue

    
def postprocess_response(output_rows, original_rows, response_text):

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

def worker(file_handle, data_queue):

    """
    Worker function for processing data items from the queue.

    Args:
        file_handle (file): File handle for writing output.
        data_queue (Queue): Queue containing data items.
    """

    while not data_queue.empty():
        data_item = data_queue.get()

        input_prompt = data_item["input_prompt"]
        original_rows = data_item["original_rows"]

        response = call_api(input_prompt)

        if response is None:
            data_queue.task_done()
            continue

        try:
            output_text = response.text
        except:
            # No text response given probably due to safety features.
            data_queue.task_done()
            continue

        # Split the output text into individual rows
        output_rows = output_text.split("\n")

        if len(output_rows) != len(original_rows):
            print(f"Number of output rows ({len(output_rows)}) does not match number of input rows ({len(original_rows)}). Skipping...")
            data_queue.task_done()
            continue

        # Postprocess the response
        processed_rows = postprocess_response(output_rows, original_rows, output_text)

        # Save the response to a file
        for row in processed_rows:
            file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        data_queue.task_done()

def main(input_folder):

    """
    Main function for processing input files.

    Args:
        input_folder (str): Path to the input folder containing seed jsonl files.
    """

    files = os.listdir(input_folder)
    save_folder = input_folder + "_output"

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    for file in files:
        filepath = os.path.join(input_folder, file)
        print(f"Processing file: {filepath}")
        
        output_filepath = os.path.join(save_folder, f"{file.split('.')[0]}_output.jsonl")

        with open(filepath, encoding='utf-8') as f:
            data = [json.loads(line) for line in f]

        data_queue = process_input(data)

        with open(output_filepath, "w", encoding='utf-8') as f:
            threads = [Thread(target=worker, args=(f, data_queue)) for i in range(MAX_THREADS)]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python genseed.py <input_folder>")
        sys.exit(1)

    input_folder = sys.argv[1]
    main(input_folder)