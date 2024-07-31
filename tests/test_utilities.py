import pytest
from src.utilities import load_csv_parse_to_dict, read_cypher_file

def test_load_csv_parse_to_dict(tmp_path):
    test_csv = tmp_path / "test_data.csv"
    test_csv.write_text("name,age\nJohn,30\nJane,25")
    
    expected_result = [
        {"name":"John", "age": 30},
        {"name": "Jane", "age":25}
    ]
    
    result = load_csv_parse_to_dict(str(test_csv))
    assert result == expected_result
    

def test_read_cypher_file(tmp_path):
    test_cypher = tmp_path / "test_query.cypher"
    query_content = "CREATE (n:Test {name: $name})"
    test_cypher.write_text(query_content)
    
    result = read_cypher_file(str(test_cypher))
    assert result == query_content
