import pandas as pd
import numpy as np
import openai
import time
from config import OPENAI_API_KEY


openai.api_key = OPENAI_API_KEY

def generate_structured_data():
    data = {
        f'col_{i}': np.random.rand(1000) * 100 if i % 2 == 0 else np.random.randint(1, 100, 1000)
        for i in range(6)
    }
    data['join_key'] = np.random.choice(['A', 'B', 'C', 'D'], size=1000)  # Key for joining
    data['pivot_index'] = np.random.choice(['Category_1', 'Category_2', 'Category_3'], size=1000)  # Index for pivoting
    data['pivot_columns'] = np.random.choice(['Type_1', 'Type_2', 'Type_3'], size=1000)  # Columns for pivoting
    data['pivot_values'] = np.random.rand(1000) * 100  # Values for pivoting
    data['date'] = pd.date_range(start='2022-01-01', periods=1000, freq='D')
    data['date2'] = pd.date_range(start='2022-01-01', periods=1000, freq='D')
    df = pd.DataFrame(data) 
    return df

structured_data = generate_structured_data()

def generate_text_entry():
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "system", "content": "Generate a short realistic text entry for an Excel dataset.It shouldnot look ai genrated, but as if someone has put text entries in  excel file"}],
        max_tokens=20  
    )
    return response['choices'][0]['message']['content'].strip()

def generate_text_entries(n):
    entries = []
    for _ in range(n):
        entries.append(generate_text_entry())
    return entries

unstructured_data = pd.DataFrame({
    f'text_col_{i}': generate_text_entries(1000)
    for i in range(5)
})


with pd.ExcelWriter('synthetic_data.xlsx') as writer:
    structured_data.to_excel(writer, sheet_name='StructuredData', index=False)
    unstructured_data.to_excel(writer, sheet_name='UnstructuredData', index=False)
