import pytest
import requests

def test_document_len():
    file= open("test_data.csv", "rb")
    files={'file':file}
    test = requests.post(
        url="http://localhost/file_uploader",
        files=files
    )
    print(test.status_code)
    print(test.headers)
    print(file)
    print(test.request)
    assert test.status_code == 200

test_document_len()