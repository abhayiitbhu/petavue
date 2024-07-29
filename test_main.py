import pytest
from fastapi.testclient import TestClient
from main import app  
import pandas as pd
import io
from datetime import datetime
from io import BytesIO

client = TestClient(app)
@pytest.fixture
def sample_excel_file():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6],
        'Date1': [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
        'Date2': [datetime(2024, 4, 1), datetime(2024, 5, 1), datetime(2024, 6, 1)]
    })
    file = io.BytesIO()
    df.to_excel(file, index=False)
    file.seek(0)  
    return file

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

def test_basic_math_operations(sample_excel_file):
    user_query_string = 'Add the values in column "A" to the values in column "B" and store the result in a new column named "A_plus_B.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    expected_sum = [5, 7, 9]
    assert "A_plus_B" in processed_df.columns
    assert (processed_df["A_plus_B"] == expected_sum).all()

def test_aggregations (sample_excel_file):
    user_query_string = 'Find avg of column "A"'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert {'result': 2.0}== response_json

    user_query_string = 'Find min of column "A" '
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert {'result': 1.0}== response_json

    user_query_string = 'Find max of column "A" and return its numeric value.'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert {'result': 3.0}== response_json

    user_query_string = 'Find sum of column "A" and return its numeric value'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert {'result': 6.0}== response_json
 
def test_date_operations(sample_excel_file):
    user_query_string = 'Extract day, month, and year from the "Date1" column and store in "year","month" and "day"' 
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    expected_year = [2024, 2024, 2024]
    expected_month = [1, 2, 3]
    expected_day = [1, 1, 1]
    assert 'year' in processed_df.columns
    assert 'month' in processed_df.columns
    assert 'day' in processed_df.columns
    assert (processed_df['year'] == expected_year).all()
    assert (processed_df['month'] == expected_month).all()
    assert (processed_df['day'] == expected_day).all()

    user_query_string = 'Calculate the difference between "Date2" and "Date1" and store it in "date_diff".'
    response = client.post(
        "/process/",
        files={"file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    print(processed_df)
    expected_date_diff = [91,90,92]  # Difference in days
    assert 'date_diff' in processed_df.columns
    assert (processed_df['date_diff'] == expected_date_diff).all()

def test_pivot_function():
    df = pd.DataFrame({
        'Category': ['A', 'A', 'B', 'B'],
        'Type': ['X', 'Y', 'X', 'Y'],
        'Value': [10, 20, 30, 40]
    })
    buffer = BytesIO()
    df.to_excel(buffer, index=False, sheet_name='Sheet1')
    buffer.seek(0)
    user_query_string = "Pivot the data using 'Category' as the index, 'Type' as the columns, and 'Value' as the values"
    response = client.post(
        "/process/",
        files={"file": ("pivot_test.xlsx", buffer, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    print(processed_df)
    expected_pivot = pd.DataFrame({
        'Category': ['A', 'B'],
        'X': [10, 30],
        'Y': [20, 40]
    }).set_index('Category').reset_index()
    pd.testing.assert_frame_equal(processed_df, expected_pivot)


def test_join_datasets(sample_excel_file, join_excel_file):
    user_query_string = 'Perform an inner join on column "A" '
    response = client.post(
        "/process/",
        files={
            "file": ("test.xlsx", sample_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "file2": ("join_test.xlsx", join_excel_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        },
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    expected_df = pd.DataFrame({
        'A': [1, 2],
        'B': [4, 5],
        'Date1': [datetime(2024, 1, 1), datetime(2024, 2, 1)],
        'Date2': [datetime(2024, 4, 1), datetime(2024, 5, 1)],
        'C': [7, 8]
    })
    pd.testing.assert_frame_equal(processed_df, expected_df)

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
        data={"query": user_query_string}
    )
    assert response.status_code == 200
    processed_file = io.BytesIO(response.content)
    processed_df = pd.read_excel(processed_file)
    expected_df = pd.DataFrame({
        'Date': ['2024-07-01', '2024-07-02', '2024-07-01', '2024-07-02'],
        'variable': ['Product_A', 'Product_A',  'Product_B','Product_B'],
        'value': [100,  200, 150,250]
    })
    pd.testing.assert_frame_equal(processed_df, expected_df)


