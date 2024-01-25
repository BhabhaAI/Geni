# GenHi
Generating Synthetic Hindi &amp; Roman Hindi Datasets

## Installation
`pip install .`

## Setup Env Variables
Setup .env file with HUGGINGFACE_TOKEN and GEMINI_API_KEY

## Generating Seed Words
1. Download dataset from HF to collect raw text `python data/seed.py`   
2. Use Gemini to extract seed words `python generate/seed.py <input-content-folder>`   
3. Generate questions using seed words `python generate/question.py <input-seed-folder>`   
4. Generate answers for the questions `python generate/answer.py <input-question-folder>`   
