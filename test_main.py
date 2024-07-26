import pytest
from fastapi.testclient import TestClient
from main import app  
import pandas as pd
from io import BytesIO
import json
import logging

client = TestClient(app)

@pytest.fixture
def sample_excel_file():
    import pandas as pd
    from io import BytesIO
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6],
        'Date': ['2024-01-01', '2024-02-01', '2024-03-01'],
        'Text': ['Hello world', 'Test message', 'Another text']
    })
    buffer = BytesIO()
    df.to_excel(buffer, index=False, sheet_name='Sheet1')
    buffer.seek(0)
    return buffer


def test_text_analysis(sample_excel_file):
    user_query_string = 'analyse the text present in Text column.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    assert response_json["file"] == "processed_text_data.xlsx"
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)
    assert "analysis" in processed_df.columns, "Column 'analysis' not found in the processed DataFrame"


@pytest.fixture
def join_excel_file():
    df = pd.DataFrame({
        'A': [1, 2, 4],
        'C': [7, 8, 9]
    })
    buffer = BytesIO()
    df.to_excel(buffer, index=False, sheet_name='Sheet1')
    buffer.seek(0)
    return buffer

def test_join_datasets(sample_excel_file, join_excel_file):
    user_query_string = 'Perform an inner join on column "A" using "join" operation.'
    response = client.post(
        "/process/",
        files={
            "file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "join_file": ("join_test.xlsx", join_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        },
        data={"user_query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    assert response_json["file"] == "processed_data.xlsx"
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)
    expected_join = pd.DataFrame({
        'A': [1, 2],
        'B': [4, 5],
        'Date': ['2024-01-01', '2024-02-01'],
        'Text': ['Hello world', 'Test message'],
        'C': [7, 8]
    })
    pd.testing.assert_frame_equal(processed_df, expected_join)

def test_pivot_data(sample_excel_file):
    df = pd.DataFrame({
        'Category': ['A', 'A', 'B', 'B'],
        'Type': ['X', 'Y', 'X', 'Y'],
        'Value': [10, 20, 30, 40]
    })
    buffer = BytesIO()
    df.to_excel(buffer, index=False, sheet_name='Sheet1')
    buffer.seek(0)
    response = client.post(
    "/process/",
    files={"file": ("pivot_test.xlsx", buffer, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    data={"user_query": "Pivot the data using 'Category' as the index, 'Type' as the columns, and 'Value' as the values"}
)
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)
    processed_df = processed_df.loc[:, ~processed_df.columns.str.contains('^Unnamed')]  # Drop any unwanted columns
    expected_pivot = pd.DataFrame({
        'Category': ['A', 'B'],
        'X': [10, 30],
        'Y': [20, 40]
    }).set_index('Category').reset_index()
    pd.testing.assert_frame_equal(processed_df, expected_pivot)


def test_unpivot_data():
    df = pd.DataFrame({
        'Date': ['2024-07-01', '2024-07-02'],
        'Product_A': [100, 200],
        'Product_B': [150, 250]
    })
    buffer = BytesIO()
    df.to_excel(buffer, index=False, sheet_name='Sheet1')
    buffer.seek(0)
    user_query_string='Unpivot the table with single "Date"  as id_vars and "Product_A" and "Product_B" as value_vars'
    response = client.post(
        "/process/",
        files={"file": ("unpivot_test.xlsx", buffer, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}

    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    assert response_json["file"] == "processed_data.xlsx"
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)    
    expected_unpivot = pd.DataFrame({
        'Date': ['2024-07-01', '2024-07-02',  '2024-07-01','2024-07-02'],
        'variable': ['Product_A', 'Product_A', 'Product_B','Product_B'],
        'value': [100, 200,150, 250]
    })
    pd.testing.assert_frame_equal(processed_df, expected_unpivot)


def test_invalid_operation(sample_excel_file):
    user_query_string = 'Perform an unknown operation.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported operation"
 

def test_date_operations(sample_excel_file):
    user_query_string = 'Perform date operations on the "Date" column.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    assert response_json["file"] == "processed_data.xlsx"
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)    
    expected_year = [2024, 2024, 2024]
    expected_month = [1, 2, 3]
    expected_day = [1, 1, 1]
    assert (processed_df['year'] == expected_year).all()
    assert (processed_df['month'] == expected_month).all()
    assert (processed_df['day'] == expected_day).all()


def test_aggregations(sample_excel_file):
    user_query_string = 'Perform aggregations on the column "A".'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert {'average': 2.0, 'max': 3.0, 'min': 1.0, 'sum': 6.0}== response_json

def test_basic_math_operations(sample_excel_file):
    user_query_string = 'Add the values in column "A" to the values in column "B" and store the result in a new column.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"user_query": user_query_string}
       
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["message"] == "Data processed"
    assert response_json["file"] == "processed_data.xlsx"      
    file_path = response_json["file"]
    processed_df = pd.read_excel(file_path)   
    expected_sum = [5, 7, 9]  
    assert (processed_df['A_plus_B'] == expected_sum).all()


