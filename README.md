# GenHi
Generating Synthetic Hindi &amp; Roman Hindi Datasets

## Installation
`pip install -r requirements.txt`

## Setup Env Variables
Setup .env file with HUGGINGFACE_TOKEN and GEMINI_API_KEY

## Generating Seed Words
1. Download dataset from HF `python seed/hf_data.py`
2. Extract seed words `python seed/genseed.py`
3. Save unique seed words `python seed/saveseed.py`
