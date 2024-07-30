import openai
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import OPENAI_API_KEY
import json
openai.api_key = OPENAI_API_KEY

def generate_data(prompt):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates synthetic data."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=8000,
        temperature=0.7
    )
    text = response.choices[0].message['content'].strip()

    # Print the raw response for debugging
    print("Raw GPT-4 response:")
    print(text)
    if "example" in text.lower() or "here's an example" in text.lower():
        raise ValueError("The response contains example data or instructions, not actual data.")
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if len(lines) < 2:
        raise ValueError("The data returned is not sufficient or correctly formatted.")
    combined_text = '\n'.join(lines)
    lines = combined_text.split('\n')
    if len(lines) < 2:
        raise ValueError("The data format is incorrect. Not enough lines.")  
    data = [[item.strip() for item in line.split('\t')] for line in lines if line.strip()]
    
    return  data

def generate_large_dataset(prompt, total_rows=1000, chunk_size=100):
    all_data = []
    num_chunks = (total_rows + chunk_size - 1) // chunk_size  
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(generate_data, prompt): i for i in range(num_chunks)}
        for future in as_completed(futures):
            try:
                data_chunk = future.result()
                if data_chunk:
                    if not all_data:
                        all_data.append(data_chunk[0])  
                    all_data.extend(data_chunk)  
            except Exception as e:
                print(f"Error generating data chunk: {e}")
    return all_data

def save_to_excel(structured_df, unstructured_df):
    with pd.ExcelWriter("synthetic_data.xlsx", engine='openpyxl') as writer:
        structured_df.to_excel(writer, sheet_name="Structured Data", index=False)
        unstructured_df.to_excel(writer, sheet_name="Unstructured Data", index=False)

def process_and_save_data(prompt, header, sheet_name):
    try:
        data = generate_large_dataset(prompt)
        data.insert(0,header)
        print(data)
        df = pd.DataFrame(data[1:], columns=data[0])
        # Print DataFrame shape and head for debugging
        print(f"DataFrame shape for {sheet_name}: {df.shape}")
        print(f"DataFrame head for {sheet_name}:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error processing {sheet_name}: {e}")
        return None
    
def generate_header(prompt):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates synthetic data headers."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=8000,
        temperature=0.7
    )
    header_String = response.choices[0].message['content'].strip()
    header= json.loads(header_String)
# Print the header for debugging
    print(header)
    if len(header) < 2:
        raise ValueError("The header line is not formatted correctly.")
    return header

def main():
    structured_header_prompt = "Generate a header row with at least 5 columns of numerical and categorical data. The columns should be related to some entity in the real world. Provide the header in list format only. No explanations or additional text."
    unstructured_header_prompt = "Generate a header row with at least 5 columns of textual data. The columns should be related to some entity in the real world like product reviews, student feedback, etc. Provide the header in list format only. No explanations or additional text."
    h1= generate_header(structured_header_prompt)
    h2= generate_header(unstructured_header_prompt)
    structured_data_prompt = f"Generate 100 rows of synthetic dataset with the  columns as {h1}. Provide the dataset in tab-separated values (TSV) format only. Dont add headers to the result.No explanations or additional text."
    unstructured_data_prompt = f"Generate 100 rows of synthetic dataset with the same columns as {h2}. Provide the dataset in tab-separated values (TSV) format only. Dont add headers to the result.No explanations or additional text."
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(process_and_save_data, structured_data_prompt, h1,"Structured Data"): "Structured Data",
            executor.submit(process_and_save_data, unstructured_data_prompt, h2, "Unstructured Data"): "Unstructured Data"
        }   
        structured_df = None
        unstructured_df = None     
        for future in as_completed(futures):
            sheet_name = futures[future]
            df = future.result()
            if df is not None:
                if sheet_name == "Structured Data":
                    structured_df = df
                else:
                    unstructured_df = df
    if structured_df is not None and unstructured_df is not None:
        save_to_excel(structured_df, unstructured_df)

if __name__ == "__main__":
    main()
