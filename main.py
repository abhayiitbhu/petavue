from fastapi import FastAPI, HTTPException, File, UploadFile,Form
import openai
import pandas as pd
import logging
import io
import numpy as np  
from fastapi.responses import StreamingResponse
from config import OPENAI_API_KEY

app = FastAPI()

logging.basicConfig(level=logging.INFO)

openai.api_key = OPENAI_API_KEY

def generate_function_code(query: str) -> str:
    messages = [
        {"role": "system", "content": "You are a helpful assistant that converts natural language queries into Python code for data manipulation."},
        {"role": "user", "content": f"Convert the following natural language query into a Python function definition that operates on a DataFrame.\n\n Provide only the code with no explanations(No single word except python code at any cost).Also In case of pivot(not unpivot) use reset_index(). Here comes the query:\n\nQuery: {query}\n\n. "}
    ]
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=messages,
        max_tokens=500,
        temperature=0
    )
    code = response.choices[0].message['content'].strip()
    if not code:
        raise HTTPException(status_code=500, detail="Failed to get code from OpenAI.")
    # clean the code
    cleaned_code = code.replace('```python', '').replace('```', '').strip()
    logging.info(f"Generated code:\n{cleaned_code}")
    return cleaned_code

def extract_function_arguments(query: str, function_code: str) -> dict:
    messages = [
        {"role": "system", "content": "You are a helpful assistant that extracts function arguments from a query and function definition."},
      {"role": "user", "content": f"""
Extract function arguments from the following query and function definition. Return the arguments as a JSON dictionary.
Query: {query}

Function Code:
{function_code}

 Extract only those arguments that are relevant to the function code. Don't worry about 'df', it is already extracted separately. If there are no other arguments to be extracted except 'df', return an empty JSON dictionary.Do not provide any explanation, just return the JSON dictionary."""}
   ]
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=messages,
        max_tokens=500,
        temperature=0
    )
    arguments = response.choices[0].message['content'].strip()
    if not arguments:
        raise HTTPException(status_code=500, detail="Failed to extract arguments from OpenAI.")
    logging.info(f"Extracted arguments:\n{arguments}")
    try:
        # Convert arguments to dictionary
        arguments_dict = eval(arguments)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse arguments: {str(e)}")
    return arguments_dict

def execute_function(code: str, df1: pd.DataFrame,df2: pd.DataFrame, **kwargs):
    try:
        exec_globals = {
            'pd': pd,  
            'np': np 
        }
        exec(code, exec_globals)
        func_name = next((name for name in exec_globals if callable(exec_globals[name])), None)
        if func_name:
            function = exec_globals[func_name]
            if 'df2' in function.__code__.co_varnames:
                result = function(df1, df2, **kwargs)
            else:
                result = function(df1, **kwargs)
            if isinstance(result, pd.DataFrame):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    result.to_excel(writer, index=False)
                output.seek(0)
                return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=result.xlsx"})
            elif isinstance(result, pd.Series):
                return result.to_frame().to_dict()
            elif isinstance(result,dict):
                    return {k: (v.item() if isinstance(v, np.generic) else v) for k, v in result.items()}
            elif isinstance(result, (int, float, str, bool)):
                return {"result": result}
            elif isinstance(result, (list, tuple)):
                return pd.DataFrame(result).to_dict()
            elif isinstance(result, np.generic):
                return {"result": result.item()}
            else:
                raise HTTPException(status_code=500, detail="Function returned a result of an unsupported type.")
        else:
            raise HTTPException(status_code=500, detail="No function found in the provided code.")
    except SyntaxError as e:
        raise HTTPException(status_code=400, detail=f"Syntax error in generated code: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing function: {str(e)}")

@app.post("/process/")
async def process_data(query: str= Form(...),file: UploadFile = File(...),file2: UploadFile = File(None)
):
    try:
        df1 = pd.read_excel(io.BytesIO(await file.read())) 
        df2 = pd.read_excel(io.BytesIO(await file2.read())) if file2 else None
        function_code = generate_function_code(query)
        arguments = extract_function_arguments(query, function_code)
        result = execute_function(function_code, df1, df2, **arguments)  
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
