import subprocess
import pytest
from unittest.mock import patch
from utils import azcopyDir 

# Mockando subprocess.run
@patch('subprocess.run')  
def test_azcopyDir(mock_run):
    source = "/path/to/source"
    destination = "/path/to/destination"
    azcopyDir(source, destination)
    
    # define command
    expected_command = ["azcopy", "copy", f"{source}/*", destination, "--recursive"]
    mock_run.assert_called_once_with(expected_command, check=True)


@patch('subprocess.run')
def test_azcopyDir_subprocess_error(mock_run):
    mock_run.side_effect = subprocess.CalledProcessError(1, 'azcopy')
    
    source = "/path/to/source"
    destination = "/path/to/destination"

    with pytest.raises(subprocess.CalledProcessError):
        azcopyDir(source, destination)

@patch('subprocess.run')
def test_azcopyDir_empty_paths(mock_run):
    with pytest.raises(ValueError) as excinfo:
        azcopyDir("", "")
    assert str(excinfo.value) == "Source and destination paths must not be empty."

@patch('subprocess.run')
def test_azcopyDir_various_inputs(mock_run):
    test_cases = [
        ("/path/to/source1", "/path/to/destination1"),
        ("/path/to/source2", "/path/to/destination2"),
        ("/path/to/source3", "/path/to/destination3"),
    ]

    for source, destination in test_cases:
        azcopyDir(source, destination)
        expected_command = ["azcopy", "copy", f"{source}/*", destination, "--recursive"]
        mock_run.assert_any_call(expected_command, check=True) 