import json
import os
import argparse

from dotenv import load_dotenv
load_dotenv()

from huggingface_hub import login
from datasets import load_dataset

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
login(token=HUGGINGFACE_TOKEN)

def save_samples(samples):
    dataset = load_dataset("uonlp/CulturaX", "hi", split='train', streaming=True)

    subrows = [item for _, item in zip(range(samples), dataset)]

    if not os.path.exists('data'):
        os.mkdir('data')

    # Save to jsonl file
    with open(f'data/{str(samples)}.jsonl', 'w', encoding='utf-8') as f:
        for entry in subrows:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

    print(f"Saved {samples} samples to data/{str(samples)}.jsonl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Save samples from uonlp/CulturaX dataset to a jsonl file.")
    parser.add_argument("--samples", type=int, required=True, help="Number of samples to save")
    args = parser.parse_args()

    save_samples(args.samples)
