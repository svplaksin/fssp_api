import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from debt_checker.utils import process_rows_concurrently

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_dataframe():
    data = {'enforcement_number': [1, 2, 3]}
    return pd.DataFrame(data)

def test_process_rows_concurrently(mock_dataframe, mock_logger):
    api_token = "fake_api_token"
    max_threads = 2
    save_interval = 1
    temp_dir = "/tmp"

    with patch('debt_checker.utils.process_row') as mock_process_row, \
         patch('debt_checker.utils.save_temp_data') as mock_save_temp_data, \
         patch('debt_checker.utils.stop_event') as mock_stop_event:

        mock_process_row.side_effect = lambda index, row, api_token, logger: f"Processed {index}"
        mock_stop_event.is_set.return_value = False

        processed_data, counter = process_rows_concurrently(
            mock_dataframe, api_token, max_threads, save_interval, temp_dir, mock_logger
        )

        assert len(processed_data) == 3
        assert counter == 3
        mock_process_row.assert_called()
        mock_save_temp_data.assert_called()

def test_process_rows_concurrently_api_error(mock_dataframe, mock_logger):
    api_token = "fake_api_token"
    max_threads = 2
    save_interval = 1
    temp_dir = "/tmp"

    with patch('debt_checker.utils.process_row') as mock_process_row, \
         patch('debt_checker.utils.save_temp_data') as mock_save_temp_data, \
         patch('debt_checker.utils.stop_event') as mock_stop_event:

        mock_process_row.side_effect = ["API_ERROR", "Processed 1", "Processed 2"]
        mock_stop_event.is_set.return_value = False

        processed_data, counter = process_rows_concurrently(
            mock_dataframe, api_token, max_threads, save_interval, temp_dir, mock_logger
        )

        assert len(processed_data) == 0
        assert counter == 0
        mock_process_row.assert_called()
        mock_save_temp_data.assert_not_called()
        mock_logger.error.assert_called_with("Stopping processing due to API error")

def test_process_rows_concurrently_stop_event(mock_dataframe, mock_logger):
    api_token = "fake_api_token"
    max_threads = 2
    save_interval = 1
    temp_dir = "/tmp"

    with patch('debt_checker.utils.process_row') as mock_process_row, \
         patch('debt_checker.utils.save_temp_data') as mock_save_temp_data, \
         patch('debt_checker.utils.stop_event') as mock_stop_event:

        mock_process_row.side_effect = lambda index, row, api_token, logger: f"Processed {index}"
        mock_stop_event.is_set.side_effect = [False, True, False]

        processed_data, counter = process_rows_concurrently(
            mock_dataframe, api_token, max_threads, save_interval, temp_dir, mock_logger
        )

        assert len(processed_data) == 1
        assert counter == 1
        mock_process_row.assert_called()
        mock_save_temp_data.assert_not_called()
        mock_logger.info.assert_called_with("Exiting from the process loop...")

def test_process_rows_concurrently_exception(mock_dataframe, mock_logger):
    api_token = "fake_api_token"
    max_threads = 2
    save_interval = 1
    temp_dir = "/tmp"

    with patch('debt_checker.utils.process_row') as mock_process_row, \
         patch('debt_checker.utils.save_temp_data') as mock_save_temp_data, \
         patch('debt_checker.utils.stop_event') as mock_stop_event:

        mock_process_row.side_effect = Exception("Test exception")
        mock_stop_event.is_set.return_value = False

        with pytest.raises(Exception, match="Test exception"):
            process_rows_concurrently(
                mock_dataframe, api_token, max_threads, save_interval, temp_dir, mock_logger
            )

        mock_logger.exception.assert_called_with("Error occurred during processing: Test exception")