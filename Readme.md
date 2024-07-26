# AI Engine for Excel Operations

This project provides an API for performing various data operations on Excel files using an AI engine. The operations include basic math operations, aggregations, joins, pivots, unpivots, and date operations.

## Table of Contents

- [Features](#features)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [API Endpoints](#api-endpoints)
  - [Example Queries](#example-queries)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Basic Math Operations**: Perform addition, subtraction, multiplication, and division on numerical columns.
- **Aggregations**: Calculate sum, average, min, max, etc., on numerical columns.
- **Joining**: Perform inner, left, right, and outer joins with another dataset.
- **Pivot and Unpivot**: Create pivot tables and unpivot them back to a normal dataset.
- **Date Operations**: Extract year, month, and day from date columns and calculate the difference between dates.

## Setup

### Prerequisites

- Python 3.7 or higher
- Docker (for containerized deployment)

### Installation

1. **Clone the repository:**

```bash
git clone https://github.com/abhayiitbhu/petavue.git
cd petavue
```



2. **Create a virtual environment:**
    ```bash
    python -m venv venv
    ```

3. **Activate the virtual environment:**
    ```bash
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

4. **Install dependencies:**
```bash
pip install -r requirements.txt

```

5. **Set up OpenAI API Key:**

Create a .env file and add your OpenAI API key:

```bash
openai.api_key = 'your-openai-api-key'

```

6. **Run the application:**

```bash
uvicorn main:app --reload
```

7. **Run the application on your browser**
Open your browser and navigate to: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) to access the Swagger UI and interactively test the API by supplying values.

## Docker Setup


1. **Build the Docker image:**

```bash
docker build -t ai-engine-excel-operations .
```

2. **Run the Docker container:**

```bash
docker run -d -p 8000:8000 ai-engine-excel-operations
```

# Usage

## API Endpoints

### Process Data

- **Endpoint**: `/process/`
- **Method**: `POST`
- **Parameters**:
  - `file`: Excel file to process
  - `user_query`: Query string describing the operation

### Example Request

#### CURL
```bash
curl -X POST "http://localhost:8000/process/" \
-H "accept: application/json" \
-H "Content-Type: multipart/form-data" \
-F "file=@path_to_your_excel_file.xlsx" \
-F "user_query=add col_0 and col_2"
```

# Example Queries

## Addition Operation

```json
{
  "user_query": "add col_0 and col_1"
}
```

## Aggregation Operation

```json
{
  "user_query": "aggregate col_0"
}
```
## Join Operation

```json
{
  "user_query": "outer join on column col_0"
}
```
## Pivot Operation

```json
{
    "user_query": "pivot table on index column 'pivot_index', columns 'pivot_columns', and values 'pivot_values'"

}
```
## Unpivot Operation

```json
{
     "user_query": "unpivot table with id_vars ['join_key', 'pivot_index', 'date', 'date2'] and value_vars ['pivot_columns', 'pivot_values']"


}
```
## Date Operation

```json
{
  "user_query": "extract year, month, and day from date column 'date'"
}

{
   "user_query": " find difference between datecolumns date and date2"
}
```

## Text Analysis

```json
{
  "user_query": " Analyse the column 'text_col_0' in sheet UnstructuredData"
}
```


