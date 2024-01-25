import os
import json
import time
from ratelimit import limits, sleep_and_retry
import google.generativeai as genai

from abc import ABC, abstractmethod

class Gemini(ABC):
    RATE_LIMIT = 45
    PERIOD = 60

    def __init__(self, api_key=None, model_name="gemini-pro"):

        if api_key is None:
            raise Exception("No API key found. Please provide an API key or set the GEMINI_API_KEY environment variable.")

        # Configure generative AI
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.generation_config = genai.types.GenerationConfig(max_output_tokens=3000, temperature=0.0)
        self.safety_settings = {'HARASSMENT':'block_none', 'HATE_SPEECH': 'block_none', 'DANGEROUS': 'block_none', 'SEXUAL': 'block_none'}
        self.equal_rows = True

    @abstractmethod
    def process_input(self):
        pass

    @abstractmethod
    def postprocess_response(self):
        pass

    @sleep_and_retry
    @limits(calls=RATE_LIMIT, period=PERIOD)
    def call_api(self, data_item, retry_count=0, max_retries=3):

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
                response = self.model.generate_content(data_item, generation_config=self.generation_config, safety_settings=self.safety_settings)
                return response
            except Exception as e:

                retry_count += 1

                if "429" in str(e): 
                    print("429 error: Rate limit exceeded. Sleeping for 80 seconds.")
                    time.sleep(80)
                    return self.call_api(data_item, retry_count, max_retries)  # Retry the same request after sleeping  

                if "500" in str(e):
                    print("500 error: Internal server error. Sleeping for 80 seconds.")
                    time.sleep(80)
                    return self.call_api(data_item, retry_count, max_retries)  # Retry the same request after sleeping
                
                print(f"API call failed: {e}, retrying... Attempt {retry_count} of {max_retries}")
                return self.call_api(data_item, retry_count, max_retries)        
        else:
            print("Maximum retries reached. Moving on to the next item.")
            return None
        
    def worker(self, file_handle, data_queue):

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

            response = self.call_api(input_prompt)

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

            if len(output_rows) != len(original_rows) and self.equal_rows:
                print(f"Number of output rows ({len(output_rows)}) does not match number of input rows ({len(original_rows)}). Skipping...")
                data_queue.task_done()
                continue

            # Postprocess the response
            processed_rows = self.postprocess_response(output_rows, original_rows, output_text)

            # Save the response to a file
            for row in processed_rows:
                file_handle.write(json.dumps(row, ensure_ascii=False) + "\n")

            data_queue.task_done()