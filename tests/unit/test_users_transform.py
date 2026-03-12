# tests/unit/test_users_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Unit tests for users transform logic
#
# KEY THINGS WE MUST VERIFY:
#   1. Nested name fields are flattened correctly
#   2. Nested address fields are flattened correctly
#   3. Geolocation strings are cast to float
#   4. Password is NEVER in the output
#   5. Missing nested fields don't crash the transform
# ═══════════════════════════════════════════════════════════

import pytest
from etl.transform.users_transform import transform_user


# ─────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────

@pytest.fixture
def fake_user():
    """
    Complete fake user exactly as API sends it.
    Includes all nested objects and the password field.
    """
    return {
        "id":       1,
        "email":    "john@gmail.com",
        "username": "johnd",
        "password": "m38rmF$",
        "name": {
            "firstname": "John",
            "lastname":  "Doe"
        },
        "address": {
            "street":  "7835 new road",
            "city":    "Ann Arbor",
            "zipcode": "48100",
            "geolocation": {
                "lat":  "-37.3159",
                "long": "81.1496"
            }
        },
        "phone": "1-570-236-7033"
    }


@pytest.fixture
def fake_user_missing_address():
    """
    User with no address field.
    Tests defensive .get() coding on nested objects.
    """
    return {
        "id":       2,
        "email":    "jane@gmail.com",
        "username": "janed",
        "password": "secret123",
        "name": {
            "firstname": "Jane",
            "lastname":  "Doe"
        },
        "phone": "1-234-567-8901"
        # address field deliberately missing
    }


@pytest.fixture
def fake_user_bad_geolocation():
    """
    User with invalid geolocation values.
    Tests try/except in geolocation parsing.
    """
    return {
        "id":       3,
        "email":    "bad@gmail.com",
        "username": "badgeo",
        "password": "secret",
        "name": {
            "firstname": "Bad",
            "lastname":  "Geo"
        },
        "address": {
            "street":  "123 main st",
            "city":    "Sydney",
            "zipcode": "2000",
            "geolocation": {
                "lat":  "not_a_number",
                "long": "also_not_a_number"
            }
        },
        "phone": "0400000000"
    }


# ─────────────────────────────────────────────────────────
# PASSWORD TESTS — most critical security tests
# ─────────────────────────────────────────────────────────

def test_transform_user_excludes_password(fake_user):
    """
    Password must NEVER appear in transform output.

    This is the most important test in this file.
    If password appears in output:
    → it reaches staging
    → it reaches warehouse
    → analysts can see it
    → security breach ❌

    Test explicitly checks password is absent ✅
    """
    result = transform_user(fake_user)
    assert "password" not in result


def test_transform_user_password_not_stored_under_any_key(
    fake_user
):
    """
    Password value must not appear as any value
    in the output dict.

    Extra paranoia test:
    What if someone accidentally stored it
    under a different key name?
    """
    result = transform_user(fake_user)
    assert "m38rmF$" not in result.values()


# ─────────────────────────────────────────────────────────
# NAME FLATTENING TESTS
# ─────────────────────────────────────────────────────────

def test_transform_user_flattens_firstname(fake_user):
    """name.firstname must become flat first_name column."""
    result = transform_user(fake_user)
    assert result["first_name"] == "John"


def test_transform_user_flattens_lastname(fake_user):
    """name.lastname must become flat last_name column."""
    result = transform_user(fake_user)
    assert result["last_name"] == "Doe"


def test_transform_user_no_nested_name_key(fake_user):
    """
    Original nested 'name' dict must not be in output.
    Only flat first_name and last_name columns exist.
    """
    result = transform_user(fake_user)
    assert "name" not in result


# ─────────────────────────────────────────────────────────
# ADDRESS FLATTENING TESTS
# ─────────────────────────────────────────────────────────

def test_transform_user_flattens_address_street(fake_user):
    """address.street must become flat address_street."""
    result = transform_user(fake_user)
    assert result["address_street"] == "7835 new road"


def test_transform_user_flattens_address_city(fake_user):
    """address.city must become flat address_city."""
    result = transform_user(fake_user)
    assert result["address_city"] == "Ann Arbor"


def test_transform_user_flattens_address_zip(fake_user):
    """address.zipcode must become flat address_zip."""
    result = transform_user(fake_user)
    assert result["address_zip"] == "48100"


def test_transform_user_no_nested_address_key(fake_user):
    """
    Original nested 'address' dict must not be in output.
    Only flat address_* columns exist.
    """
    result = transform_user(fake_user)
    assert "address" not in result


# ─────────────────────────────────────────────────────────
# GEOLOCATION TESTS
# ─────────────────────────────────────────────────────────

def test_transform_user_casts_lat_to_float(fake_user):
    """
    Geolocation lat comes as string from API.
    Must be cast to float for NUMERIC(9,6) column.
    """
    result = transform_user(fake_user)
    assert isinstance(result["address_lat"], float)


def test_transform_user_casts_lng_to_float(fake_user):
    """Geolocation long must be cast to float."""
    result = transform_user(fake_user)
    assert isinstance(result["address_lng"], float)


def test_transform_user_correct_lat_value(fake_user):
    """Lat value must be correctly parsed from string."""
    result = transform_user(fake_user)
    assert result["address_lat"] == -37.3159


def test_transform_user_correct_lng_value(fake_user):
    """Lng value must be correctly parsed from string."""
    result = transform_user(fake_user)
    assert result["address_lng"] == 81.1496


def test_transform_user_bad_geolocation_returns_none(
    fake_user_bad_geolocation
):
    """
    Invalid geolocation strings must not crash pipeline.
    Must return None for lat and lng instead.

    WHY test this?
    Real APIs sometimes send "N/A" or "" for geolocation
    float("not_a_number") → ValueError
    Our try/except must catch this gracefully ✅
    """
    result = transform_user(fake_user_bad_geolocation)
    assert result["address_lat"] is None
    assert result["address_lng"] is None


# ─────────────────────────────────────────────────────────
# MISSING FIELDS TESTS
# ─────────────────────────────────────────────────────────

def test_transform_user_handles_missing_address(
    fake_user_missing_address
):
    """
    If address field is missing entirely:
    Must not crash.
    All address fields must be None.
    """
    result = transform_user(fake_user_missing_address)

    assert result["address_street"] is None
    assert result["address_city"]   is None
    assert result["address_zip"]    is None
    assert result["address_lat"]    is None
    assert result["address_lng"]    is None


def test_transform_user_output_has_correct_keys(fake_user):
    """
    Output must have exactly the right keys.
    No password, no nested dicts, no missing columns.
    """
    result = transform_user(fake_user)

    expected_keys = {
        "source_id",
        "email",
        "username",
        "first_name",
        "last_name",
        "phone",
        "address_street",
        "address_city",
        "address_zip",
        "address_lat",
        "address_lng",
    }

    assert set(result.keys()) == expected_keys


def test_transform_user_correct_source_id(fake_user):
    """source_id must equal the API's original id."""
    result = transform_user(fake_user)
    assert result["source_id"] == 1


def test_transform_user_preserves_email(fake_user):
    """Email must pass through unchanged."""
    result = transform_user(fake_user)
    assert result["email"] == "john@gmail.com"


def test_transform_user_preserves_username(fake_user):
    """Username must pass through unchanged."""
    result = transform_user(fake_user)
    assert result["username"] == "johnd"