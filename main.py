from fastapi import FastAPI, UploadFile, HTTPException, Form
from pydantic import BaseModel
import pandas as pd
import openai
import io
import json
import logging
from config import OPENAI_API_KEY
import ast
from fastapi.middleware.cors import CORSMiddleware

from typing import Dict, Optional, List


openai.api_key = OPENAI_API_KEY

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.DEBUG)

class OperationParams(BaseModel):
    column1: Optional[str] = None
    column2: Optional[str] = None
    operation: str
    column: Optional[str] = None
    index: Optional[str] = None
    columns: Optional[str] = None
    values: Optional[str] = None
    id_vars: Optional[List[str]] = None
    value_vars: Optional[List[str]] = None
    date_column: Optional[str] = None
    date_column2: Optional[str] = None
    text_column: Optional[str] = None
    how: str = 'inner'
    sheet_name:Optional[str]=None

def read_excel(file: UploadFile):
    try:
        content = file.file.read()
        return pd.read_excel(io.BytesIO(content), sheet_name=None)
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        raise HTTPException(status_code=400, detail="Error reading Excel file")

def basic_math_operations(df, column1, column2, operation):
    if column1 not in df.columns or column2 not in df.columns:
        raise HTTPException(status_code=400, detail=f"Columns '{column1}' or '{column2}' not found in DataFrame.")
    
    if not pd.api.types.is_numeric_dtype(df[column1]) or not pd.api.types.is_numeric_dtype(df[column2]):
        raise HTTPException(status_code=400, detail=f"Columns '{column1}' and '{column2}' must be numeric.")
    try:
        if operation == 'add':
            df[f'{column1}_plus_{column2}'] = df[column1] + df[column2]
        elif operation == 'subtract':
            df[f'{column1}_minus_{column2}'] = df[column1] - df[column2]
        elif operation == 'multiply':
            df[f'{column1}_times_{column2}'] = df[column1] * df[column2]
        elif operation == 'divide':
            df[f'{column1}_divided_by_{column2}'] = df[column1] / df[column2]
        else:
            raise HTTPException(status_code=400, detail="Unsupported math operation")
    except Exception as e:
        logging.error(f"Error in basic_math_operations: {str(e)}")
        raise HTTPException(status_code=500, detail="Error performing operation")
    return df

def calculate_aggregations(df, column):
    if column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column}' not found in DataFrame.")
    if not pd.api.types.is_numeric_dtype(df[column]):
        raise HTTPException(status_code=400, detail=f"Column '{column}' must be numeric.")
    try:
        summary = {
            'sum': float(df[column].sum()),
            'average': float(df[column].mean()),
            'min': float(df[column].min()),
            'max': float(df[column].max())
        }
    except Exception as e:
        logging.error(f"Error in calculate_aggregations: {str(e)}")
        raise HTTPException(status_code=500, detail="Error calculating aggregations")
    return summary

def join_datasets(df1, df2, on_column, how='inner'):
    if on_column not in df1.columns or on_column not in df2.columns:
        raise HTTPException(status_code=400, detail=f"Column '{on_column}' not found in one of the DataFrames.")
    try:
        return pd.merge(df1, df2, on=on_column, how=how)
    except Exception as e:
        logging.error(f"Error in join_datasets: {str(e)}")
        raise HTTPException(status_code=500, detail="Error performing join")

def pivot_data(df, index: str, columns: str, values: str):
    try:
        ans = df.pivot_table(index=index, columns=columns, values=values)
        ans = ans.reset_index()  
        return ans
    except Exception as e:
        logging.error(f"Error in pivot_data: {str(e)}")
        raise HTTPException(status_code=500, detail="Error performing pivot")

def unpivot_data(df, id_vars: str, value_vars: List[str]):
    try:
        ans = df.melt(id_vars=id_vars, value_vars=value_vars)
        return ans
    except Exception as e:
        logging.error(f"Error in unpivot_data: {str(e)}")
        raise HTTPException(status_code=500, detail="Error performing unpivot")

def date_operations(df: pd.DataFrame, date_column: str, date_column2: Optional[str] = None) -> pd.DataFrame:
    df[date_column] = pd.to_datetime(df[date_column])
    if date_column2:
        df[date_column2] = pd.to_datetime(df[date_column2])
        df['year2'] = df[date_column2].dt.year
        df['month2'] = df[date_column2].dt.month
        df['day2'] = df[date_column2].dt.day
        df['date_diff'] = (df[date_column] - df[date_column2]).dt.days
    else:
        df['year'] = df[date_column].dt.year
        df['month'] = df[date_column].dt.month
        df['day'] = df[date_column].dt.day
    return df


def analyze_text(text):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": f"Analyze the following text: {text} and give feedback analysis (positive or negative), text summary"}],
            max_tokens=50
        )
        return response['choices'][0]['message']['content'].strip()
    except Exception as e:
        logging.error(f"Error in analyze_text: {str(e)}")
        raise HTTPException(status_code=500, detail="Error analyzing text")

def add_text_analysis(df, text_column):
    if text_column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{text_column}' not found in DataFrame.")
    try:
        df['analysis'] = df[text_column].apply(analyze_text)
    except Exception as e:
        logging.error(f"Error in add_text_analysis: {str(e)}")
        raise HTTPException(status_code=500, detail="Error adding text analysis")
    return df



def extract_params(response: str) -> Dict[str, Optional[str]]:
    """
    Extracts parameters from the OpenAI API response text.

    :param response: The response text from OpenAI containing parameters.
    :return: A dictionary with parameter names and their values.
    """

    params = {
        'operation': None,
        'column1': None,
        'column2': None,
        'column': None,
        'index': None,
        'columns': None,
        'values': None,
        'id_vars': None,
        'value_vars': None,
        'date_column': None,
        'date_column2': None,
        'text_column': None,
        'how': 'inner',
        'sheet_name':None
    }

    try:
        parsed_response = json.loads(response)
        for key in params.keys():
            if key in parsed_response:
                params[key] = parsed_response[key]
        if 'value_vars' in params and isinstance(params['value_vars'], str):
            params['value_vars'] = [v.strip() for v in params['value_vars'].split(',')]
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response: {str(e)}")
        raise ValueError("Invalid JSON response from OpenAI API")
    return params


def extract_params(user_query: str):
    try:
        prompt = f'''
I need to perform a data operation on a DataFrame based on the following query:
"{user_query}"

Please extract and provide the operation parameters in the following format, considering these definitions:

- **operation**: Specifies the type of operation to be performed. Possible values are ['add', 'subtract', 'multiply', 'divide', 'aggregations', 'join', 'pivot', 'unpivot', 'date_operations', 'text_analysis'].
- **column1**: The first column involved in the operation. (Only applicable for 'add', 'subtract', 'multiply', 'divide') will remain None in case of any other operations
- **column2**: The second column involved in the operation, if applicable. (Only applicable for 'add', 'subtract', 'multiply', 'divide')  will remain None in case of any other operations
- **column**: The column to be used if the operation involves an aggregation or join; (Only applicable for 'join' and 'aggregations')  will remain None in case of any other operations
- **index**: The column to be used as the index (only for pivoting).will remain None in case of any other operations
- **columns**: Columns to be used in the operation (only for pivoting). will remain None in case of any other operations
- **values**: The column whose values will be used in the operation (only for pivoting).will remain None in case of any other operations
- **id_vars**: Columns to retain as identifier variables (for unpivoting). will remain None in case of any other operations
- **value_vars**: Columns to unpivot into a single column (for unpivoting). will remain None in case of any other operations
- **date_column**: A column containing date information. (For 'date_operations' ) but not for anything else. first column for date come here
- **date_column2**: A second date column, if applicable. (For 'date_operations') but not for anything else.second columns for date come here
- **text_column**: A column containing textual data for 'text_analysis'. only for text_analysis
- **how**: Type of join operation (e.g., "inner", "outer", "left", "right"). If not specified, default to "inner".else if user says for example"left join" then its value mustbe left
- **sheet_name**: Name of the sheet in the Excel file if provided; else, it should be None.


this is the pydantic model which it should follow :
class OperationParams(BaseModel):
    column1: Optional[str] = None
    column2: Optional[str] = None
    operation: str
    column: Optional[str] = None
    index: Optional[str] = None
    columns: Optional[str] = None
    values: Optional[str] = None
    id_vars: Optional[List[str]] = None
    value_vars: Optional[List[str]] = None
    date_column: Optional[str] = None
    date_column2: Optional[str] = None
    text_column: Optional[str] = None
    how: str = Union['inner','outer','left','right]
    sheet_name: str= None
'''
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Fill in the following parameter structure based on the user query and return it as a dictionary only."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=200
        )
        parsed_response = response['choices'][0]['message']['content'].strip()
        logging.debug(f"OpenAI API response: {parsed_response}")
        if isinstance(parsed_response, str):
            try:
                operation_params_dict = json.loads(parsed_response)
            except json.JSONDecodeError:
                operation_params_dict = ast.literal_eval(parsed_response)
                logging.debug(f"Extracted operation parameters: {operation_params_dict}")
        elif isinstance(parsed_response, dict):
            operation_params_dict = parsed_response
        else:
            logging.error("Unexpected response format.")
            raise ValueError("Unexpected response format.")
        logging.debug(f"Extracted operation parameters: {operation_params_dict}")
        return operation_params_dict
    except Exception as e:
        logging.error(f"Error calling OpenAI API or extracting parameters: {str(e)}")
        raise

@app.post("/process/")
async def process_data(
    file: UploadFile,
    user_query: str = Form(...),
    join_file: Optional[UploadFile] = None,
):
    try:
        try:
            operation_params_dict = extract_params(user_query)
            operation_params = OperationParams(**operation_params_dict)
            print(operation_params)
            dfs = read_excel(file)
            if not operation_params.sheet_name:
                sheet_name = list(dfs.keys())[0]
            else:
                sheet_name= operation_params.sheet_name
            df = dfs[sheet_name]
        except Exception as e:
            logging.error(f"Error calling OpenAI API or extracting parameters: {str(e)}")
            raise HTTPException(status_code=500, detail="Error parsing query with OpenAI API")
        operation = operation_params.operation
        if operation in ['add', 'subtract', 'multiply', 'divide']:
            if not operation_params.column1 or not operation_params.column2:
                raise HTTPException(status_code=400, detail="Columns 'column1' and 'column2' are required for this operation")
            df = basic_math_operations(df, operation_params.column1, operation_params.column2, operation_params.operation)   
        elif operation == 'aggregations':
            if not operation_params.column:
                raise HTTPException(status_code=400, detail="Column is required for aggregation")
            summary = calculate_aggregations(df, operation_params.column)
            return summary
        elif operation == 'join':
            if not join_file:
                raise HTTPException(status_code=400, detail="Join file is required")
            join_dfs = read_excel(join_file)
            join_sheet_name = list(join_dfs.keys())[0]
            join_df = join_dfs[join_sheet_name]
            df = join_datasets(df, join_df, operation_params.column, operation_params.how)
        elif operation == 'pivot':
            if not operation_params.index or not operation_params.columns or not operation_params.values:
                raise HTTPException(status_code=400, detail="Index, columns, and values are required for pivot")
            index = operation_params.index
            columns = operation_params.columns
            values = operation_params.values
            df = pivot_data(df, index, columns, values)
        elif operation == 'unpivot':
            if not operation_params.id_vars or not operation_params.value_vars:
                raise HTTPException(status_code=400, detail="Id_vars and value_vars are required for unpivot")
            id_vars = operation_params.id_vars
            value_vars = operation_params.value_vars
            if isinstance(id_vars, str):
                id_vars = [id_vars]
            if isinstance(value_vars, str):
                value_vars = [value_vars]
            df = unpivot_data(df, id_vars, value_vars)
        elif operation == 'date_operations':
            if(operation_params.column1):
                operation_params.date_column=operation_params.column1
            if(operation_params.column2):
                operation_params.date_column2=operation_params.column2
            if not operation_params.date_column:
                raise HTTPException(status_code=400, detail="Date column is required for date operations")
            df = date_operations(df, operation_params.date_column, operation_params.date_column2)
        elif operation == 'text_analysis':
            if not operation_params.text_column:
                raise HTTPException(status_code=400, detail="Text column is required for text analysis")
            df = add_text_analysis(df, operation_params.text_column)
        else:
            raise HTTPException(status_code=400, detail="Unsupported operation")
        if operation== 'text_analysis':
            output_file = "processed_text_data.xlsx"
        else:
            output_file = "processed_data.xlsx"
        try:
            if operation == 'pivot':
                df.to_excel(output_file, index=False) 
            else:
                df.to_excel(output_file, index=False)
        except Exception as e:
            logging.error(f"Error saving Excel file: {str(e)}")
            raise HTTPException(status_code=500, detail="Error saving processed data") 
        return {"message": "Data processed", "file": output_file}
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logging.error(f"Error in process_data: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")
