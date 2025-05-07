# test_utils.py
import pytest
import pandas as pd
import os
from unittest.mock import MagicMock, patch
from utils import (
    load_input_data,
    process_row,
    process_rows_concurrently,
    save_temp_data,
    merge_temp_files,
    save_dataframe_to_excel,
    ProcessResult, setup_signal_handler
)

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'number': ['123', '456', '789'],
        'Debt Amount': [None, 100.0, None]
    })

@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path / "temp_files"


def test_load_input_data_success(tmp_path, mock_logger):
    # Create test Excel file
    test_file = tmp_path / "test.xlsx"
    df = pd.DataFrame({'number': ['123', '456'], 'Debt Amount': [None, None]})
    df.to_excel(test_file, index=False)

    # Test loading
    result = load_input_data(str(test_file), mock_logger)
    assert len(result) == 2
    assert 'Debt Amount' in result.columns
    mock_logger.info.assert_called()


def test_load_input_data_missing_file(mock_logger):
    with pytest.raises(SystemExit):
        load_input_data("nonexistent.xlsx", mock_logger)
    mock_logger.error.assert_called()


def test_load_input_data_adds_debt_column(tmp_path, mock_logger):
    # Create test file without Debt Amount column
    test_file = tmp_path / "test.xlsx"
    pd.DataFrame({'number': ['123']}).to_excel(test_file, index=False)

    result = load_input_data(str(test_file), mock_logger)
    assert 'Debt Amount' in result.columns


def test_process_row_existing_data(sample_dataframe, mock_logger):
    row = sample_dataframe.iloc[1]  # Has existing debt
    result = process_row(0, row, "test_token", mock_logger)
    assert result is None
    mock_logger.info.assert_called()

def test_process_row_api_success(mock_logger):
    test_row = pd.Series(['123'], index=['number'])
    with patch('utils.get_debt_amount', return_value=500.0):
        result = process_row(0, test_row, "test_token", mock_logger)
        assert result.debt_amount == 500.0

def test_process_row_api_error(mock_logger):
    test_row = pd.Series(['123'], index=['number'])
    with patch('utils.get_debt_amount', return_value="TOKEN_NO_ACCESS"):
        result = process_row(0, test_row, "test_token", mock_logger)
        assert result.debt_amount == "TOKEN_NO_ACCESS"
        mock_logger.error.assert_called()





def test_save_temp_data(temp_dir, mock_logger):
    os.makedirs(temp_dir, exist_ok=True)
    test_data = [ProcessResult(0, '123', 100.0)]

    save_temp_data(test_data, 1, mock_logger, temp_dir)

    files = os.listdir(temp_dir)
    assert len(files) == 1
    assert "temp_1.csv" in files[0]
    mock_logger.info.assert_called()


def test_merge_temp_files(sample_dataframe, temp_dir, mock_logger):
    os.makedirs(temp_dir, exist_ok=True)

    # Create test temp files
    temp1 = pd.DataFrame({'number': ['123'], 'debt_amount': [100.0]})
    temp1.to_csv(temp_dir / "numbers_with_debt_temp_1.csv", index=False)

    result = merge_temp_files(temp_dir, sample_dataframe, "output.xlsx", mock_logger)

    assert result is not None
    assert os.path.exists("output.xlsx")
    assert len(result) == 3


def test_save_dataframe_to_excel(tmp_path, mock_logger):
    test_file = tmp_path / "output.xlsx"
    df = pd.DataFrame({'number': ['123']})

    save_dataframe_to_excel(df, test_file, logger=mock_logger)

    assert os.path.exists(test_file)
    mock_logger.info.assert_called()


def test_save_dataframe_to_excel_error(mock_logger):
    with patch('pandas.DataFrame.to_excel', side_effect=Exception("Test error")):
        with pytest.raises(Exception):
            save_dataframe_to_excel(pd.DataFrame(), "test.xlsx", logger=mock_logger)
        mock_logger.exception.assert_called()

