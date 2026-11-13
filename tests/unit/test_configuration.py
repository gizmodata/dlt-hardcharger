"""The destination's configuration: the URL, its parts, and the location dlt records."""

from __future__ import annotations

import pytest
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationValueError
from sqlalchemy_hardcharger import HardchargerDialect

from dlt_hardcharger import HardchargerClientConfiguration, HardchargerCredentials


def _configuration(credentials: object) -> HardchargerClientConfiguration:
    configuration = HardchargerClientConfiguration(credentials=credentials)  # type: ignore[arg-type]
    configuration = configuration._bind_dataset_name(dataset_name="raw")
    return resolve_configuration(configuration, sections=("destination", "hardcharger"))


def test_url_parts() -> None:
    credentials = _configuration(
        credentials="hardcharger://ada:s3cret@db.example.com:8443/lake?tls=false"
    ).credentials
    assert credentials.drivername == "hardcharger"
    assert (credentials.username, credentials.password) == ("ada", "s3cret")
    assert (credentials.host, credentials.port, credentials.database) == (
        "db.example.com",
        8443,
        "lake",
    )
    assert credentials.query == {"tls": "false"}
    assert credentials.get_backend_name() == "hardcharger"
    assert credentials.get_dialect_class() is HardchargerDialect
    assert "s3cret" not in str(credentials)


def test_parts_need_no_drivername() -> None:
    configuration = _configuration(
        credentials=HardchargerCredentials(
            {"host": "db.example.com", "username": "ada", "password": "s3cret"}
        )
    )
    assert (
        configuration.credentials.to_native_representation()
        == "hardcharger://ada:s3cret@db.example.com"
    )


@pytest.mark.parametrize(
    ("url", "location"),
    [
        ("hardcharger://u:p@db.example.com", "hardcharger://db.example.com"),
        ("hardcharger://u:p@db.example.com:8443", "hardcharger://db.example.com:8443"),
        ("hardcharger://u:p@db.example.com/lake", "hardcharger://db.example.com/lake"),
        (
            "hardcharger://u:p@db.example.com:8443/lake?tls=false",
            "hardcharger://db.example.com:8443/lake",
        ),
    ],
)
def test_data_location(url: str, location: str) -> None:
    configuration = _configuration(credentials=url)
    assert configuration.data_location() == location
    assert configuration.destination_type == "hardcharger"
    assert "u:p" not in location


def test_fingerprint_is_of_the_server_only() -> None:
    one = _configuration(credentials="hardcharger://u:p@db.example.com/lake").fingerprint()
    other_user = _configuration(credentials="hardcharger://x:y@db.example.com/main").fingerprint()
    other_host = _configuration(credentials="hardcharger://u:p@db2.example.com/lake").fingerprint()
    assert one == other_user != other_host
    assert one


def test_another_scheme_is_refused() -> None:
    with pytest.raises(ConfigurationValueError) as raised:
        _configuration(credentials="postgresql://u:p@db.example.com/app")
    assert "takes a hardcharger:// URL, not `postgresql://`" in str(raised.value)


def test_a_url_without_a_host_is_refused() -> None:
    with pytest.raises(ConfigurationValueError) as raised:
        _configuration(
            credentials=HardchargerCredentials({"username": "ada", "password": "s3cret"})
        )
    assert "has no server to connect to" in str(raised.value)
    assert "DESTINATION__HARDCHARGER__CREDENTIALS" in str(raised.value)


def test_a_connection_profile_needs_no_host() -> None:
    configuration = _configuration(credentials="hardcharger://?profile=warehouse")
    assert configuration.credentials.query == {"profile": "warehouse"}


def test_the_dialect_is_known_before_the_credentials_are() -> None:
    """dlt asks for capabilities when the credentials are absent or still the URL's text."""
    assert HardchargerClientConfiguration().get_dialect_class() is HardchargerDialect
    assert HardchargerClientConfiguration().get_backend_name() == "hardcharger"
    assert HardchargerCredentials().get_dialect_class() is HardchargerDialect
