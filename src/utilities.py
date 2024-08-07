import pandas as pd


def load_csv_parse_to_dict(file_path: str) -> dict:
    """Loads csv file and parses it as a dictionary."""
    return pd.read_csv(file_path).to_dict(orient="records")


def read_cypher_file(file_path: str) -> str:
    """Read and return the contents of a cypher file."""
    with open(file_path, "r") as file:
        text: str = file.read()
    return text
