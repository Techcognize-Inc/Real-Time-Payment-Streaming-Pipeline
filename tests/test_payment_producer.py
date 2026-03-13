import sys                           ## Used to manipulate Python runtime environment, specifically to modify the module search path
from pathlib import Path             ## Used for handling file paths in a platform-independent way
from datetime import datetime        ## Used to parse and validate ISO formatted date strings in the generated payment records
from unittest.mock import MagicMock  ## Used for creating mock objects to replace real dependencies during testing

# Add producer folder to Python path so pytest can find payment_producer.py
sys.path.append(str(Path.cwd() / "airflow" / "dags" / "producer"))

import payment_producer  # Import your producer module after adding path


def test_generate_payment_returns_dict():
    payment = payment_producer.generate_payment()  # Generate one payment record
    assert isinstance(payment, dict)  # Payment should be a dictionary


def test_generate_payment_has_required_fields():
    payment = payment_producer.generate_payment()  # Generate one payment record

    expected_keys = {
        "transaction_id",
        "bank_code",
        "amount",
        "payment_type",
        "status",
        "event_time",
    }  # These are the mandatory fields expected in every payment record

    assert expected_keys.issubset(payment.keys())  # Check all required keys exist


def test_transaction_id_is_non_empty_string():
    payment = payment_producer.generate_payment()  # Generate one payment record

    assert isinstance(payment["transaction_id"], str)  # transaction_id should be string
    assert len(payment["transaction_id"]) > 0  # transaction_id should not be empty


def test_bank_code_is_valid():
    payment = payment_producer.generate_payment()  # Generate one payment record

    assert payment["bank_code"] in ["HDFC", "ICICI", "SBI", "AXIS"]  # bank_code must be from allowed list


def test_amount_is_in_expected_range():
    payment = payment_producer.generate_payment()  # Generate one payment record

    assert isinstance(payment["amount"], float)  # amount should be float
    assert 100 <= payment["amount"] <= 50000  # amount should be within expected business range


def test_payment_type_is_valid():
    payment = payment_producer.generate_payment()  # Generate one payment record

    assert payment["payment_type"] in ["NEFT", "IMPS", "RTGS"]  # payment_type must be valid


def test_status_is_valid():
    payment = payment_producer.generate_payment()  # Generate one payment record

    assert payment["status"] in ["SUCCESS", "FAILED"]  # status should be one of these two values


def test_event_time_is_valid_iso_format():
    payment = payment_producer.generate_payment()  # Generate one payment record

    parsed_time = datetime.fromisoformat(payment["event_time"])  # Try parsing event_time
    assert parsed_time is not None  # Parsing should succeed if format is valid ISO


def test_run_producer_sends_expected_number_of_messages(monkeypatch):  ##Monkeypatch allows us to replace parts of our code with mock objects during testing
    mock_producer_class = MagicMock()      # fake KafkaProducer class
    mock_instance = mock_producer_class.return_value   # instance returned when called

    monkeypatch.setattr(payment_producer, "KafkaProducer", mock_producer_class)  
    # Replace real producer object with mocked producer

    monkeypatch.setattr(payment_producer.time, "sleep", lambda x: None)  
    # Disable sleep during test so it runs fast

    payment_producer.run_producer()  
    # Run producer function; this should send 1000 records as defined in your code

    assert mock_instance.send.call_count == 1000  
    # send() should be called exactly 1000 times


def test_run_producer_uses_correct_topic(monkeypatch):
    mock_producer_class = MagicMock()      # fake KafkaProducer class
    mock_instance = mock_producer_class.return_value   # instance returned when called

    monkeypatch.setattr(payment_producer, "KafkaProducer", mock_producer_class)  
    # Replace real producer with fake one

    monkeypatch.setattr(payment_producer.time, "sleep", lambda x: None)  
    # Skip actual sleep

    payment_producer.run_producer()  
    # Execute producer logic

    topic = mock_instance.send.call_args[0][0]     ## Topic name that producer tries to send to is the first argument of the first call to send()

    assert topic == "payments_raw"