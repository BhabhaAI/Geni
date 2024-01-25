import os
from threading import Thread
from queue import Queue
import json
import sys
from dotenv import load_dotenv
load_dotenv()

from models.gemini import Gemini

class GeminiQuestion(Gemini):

    def __init__(self, api_key=None, model_name="gemini-pro"):
        super().__init__(api_key, model_name)

        self.INPUT_PROMPT = ''' Generate a question using the given seed words.
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
                text = row["seed"]
                input_prompt = self.INPUT_PROMPT + f"\nSeeds: {text}\nQuestion:"
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
                original_rows[idx]["question"] = row.strip()

            except Exception as e:
                print(f"Error parsing output row: {e}")
                original_rows[idx]["question"] = ""

        # Also put original response for safe keeping
        for idx, row in enumerate(original_rows):
            original_rows[idx]["question_raw"] = response_text

        return original_rows
    
def main(input_folder, max_threads=10):

    """
    Main function for processing input files.

    Args:
        input_folder (str): Path to the input folder containing seed jsonl files.
    """

    api_key = os.getenv("GEMINI_API_KEY")
    question_generator = GeminiQuestion(api_key=api_key)

    files = os.listdir(input_folder)
    files = [file for file in files if file.endswith(".jsonl")]
    save_folder = input_folder + "_questions"

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)


    for file in files:

        filepath = os.path.join(input_folder, file)
        print(f"Processing file: {filepath}")
        
        output_filepath = os.path.join(save_folder, f"{file.split('.')[0]}_output.jsonl")

        with open(filepath, encoding='utf-8') as f:
            data = [json.loads(line) for line in f]

        data_queue = question_generator.process_input(data)

        with open(output_filepath, "w", encoding='utf-8') as f:
            threads = [Thread(target=question_generator.worker, args=(f, data_queue)) for i in range(max_threads)]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate/question.py <input_folder>")
        sys.exit(1)

    input_folder = sys.argv[1]
    main(input_folder)