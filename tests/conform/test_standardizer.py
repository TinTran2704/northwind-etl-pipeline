"""Tests for src/conform/standardizer.py."""

import pandas as pd
import pytest
import yaml

from src.conform.standardizer import Standardizer


def _standardizer(tmp_path) -> Standardizer:
    std_dir = tmp_path / "standardization"
    std_dir.mkdir()
    # Minimal country_aliases.yaml
    aliases = {
        "country_aliases": {
            "Germany": "DE",
            "USA": "US",
            "United States": "US",
            "UK": "GB",
            "France": "FR",
            "Brasil": "BR",
            "Brazil": "BR",
            "Holland": "NL",
            "Netherlands": "NL",
            "Mexico": "MX",
        },
        "country_info": {
            "DE": {"country_name": "Germany", "region": "Europe",
                   "subregion": "Western Europe", "primary_currency": "EUR"},
            "US": {"country_name": "United States", "region": "Americas",
                   "subregion": "Northern America", "primary_currency": "USD"},
        },
    }
    (std_dir / "country_aliases.yaml").write_text(yaml.dump(aliases))
    # Minimal entity_rules.yaml
    rules = {
        "entities": {
            "customers": [
                {"column": "country", "transform": "standardize_country", "output_column": "country_code"},
                {"column": "city", "transform": "title_case"},
                {"column": "phone", "transform": "standardize_phone", "country_column": "country_code"},
            ]
        }
    }
    (std_dir / "entity_rules.yaml").write_text(yaml.dump(rules))
    return Standardizer(config_dir=tmp_path)


class TestStandardizeCountry:
    def test_usa_to_us(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("USA") == "US"

    def test_uk_to_gb(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("UK") == "GB"

    def test_brasil_to_br(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("Brasil") == "BR"

    def test_holland_to_nl(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("Holland") == "NL"

    def test_canonical_name_works(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("Germany") == "DE"

    def test_unknown_country_returned_unchanged(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("Freedonia") == "Freedonia"

    def test_none_returned_as_none(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country(None) is None

    def test_empty_string_returned_unchanged(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_country("") == ""


class TestStandardizePhone:
    def test_us_format(self, tmp_path):
        s = _standardizer(tmp_path)
        result = s.standardize_phone("(503) 555-7555", "US")
        assert result == "+1-5035557555"

    def test_german_format(self, tmp_path):
        s = _standardizer(tmp_path)
        result = s.standardize_phone("030-0074321", "DE")
        assert result == "+49-0300074321"

    def test_unknown_country_digits_only(self, tmp_path):
        s = _standardizer(tmp_path)
        result = s.standardize_phone("123-456", "XX")
        assert result == "123456"

    def test_none_returned_as_none(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.standardize_phone(None, "US") is None

    def test_strips_all_non_digits(self, tmp_path):
        s = _standardizer(tmp_path)
        result = s.standardize_phone("+1 (800) 555.0199", "US")
        assert result == "+1-18005550199"


class TestTitleCase:
    def test_upper_to_title(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.title_case("BERLIN") == "Berlin"

    def test_lower_to_title(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.title_case("london") == "London"

    def test_mixed_unchanged(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.title_case("New York") == "New York"

    def test_none_returned_as_none(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.title_case(None) is None

    def test_strips_whitespace(self, tmp_path):
        s = _standardizer(tmp_path)
        assert s.title_case("  paris  ") == "Paris"


class TestStandardizeDf:
    def _customers_df(self):
        return pd.DataFrame({
            "customerID": ["ALFKI", "ANATR"],
            "companyName": ["ALFREDS FUTTERKISTE", "ana trujillo"],
            "city": ["berlin", "MEXICO D.F."],
            "country": ["Germany", "Mexico"],
            "phone": ["030-0074321", "(5) 555-4729"],
        })

    def test_country_column_standardized(self, tmp_path):
        s = _standardizer(tmp_path)
        out = s.standardize_df(self._customers_df(), "customers")
        assert "country_code" in out.columns
        assert out.loc[0, "country_code"] == "DE"

    def test_city_title_cased(self, tmp_path):
        s = _standardizer(tmp_path)
        out = s.standardize_df(self._customers_df(), "customers")
        assert out.loc[0, "city"] == "Berlin"

    def test_phone_standardized_after_country_code(self, tmp_path):
        s = _standardizer(tmp_path)
        out = s.standardize_df(self._customers_df(), "customers")
        assert out.loc[0, "phone"].startswith("+49-")

    def test_input_df_not_modified(self, tmp_path):
        s = _standardizer(tmp_path)
        df = self._customers_df()
        original_city = df["city"].iloc[0]
        s.standardize_df(df, "customers")
        assert df["city"].iloc[0] == original_city

    def test_unknown_entity_returns_copy(self, tmp_path):
        s = _standardizer(tmp_path)
        df = pd.DataFrame({"A": [1, 2]})
        out = s.standardize_df(df, "nonexistent_entity")
        assert list(out["A"]) == [1, 2]

    def test_missing_column_skipped_gracefully(self, tmp_path):
        s = _standardizer(tmp_path)
        # df has no 'phone' column
        df = pd.DataFrame({"customerID": ["X"], "country": ["Germany"]})
        out = s.standardize_df(df, "customers")
        assert "country_code" in out.columns  # country was transformed
        assert "phone" not in out.columns     # not added for missing column


class TestGetCountryInfo:
    def test_returns_country_info_dict(self, tmp_path):
        s = _standardizer(tmp_path)
        info = s.get_country_info()
        assert "DE" in info
        assert info["DE"]["country_name"] == "Germany"
        assert info["DE"]["region"] == "Europe"
        assert info["DE"]["primary_currency"] == "EUR"

    def test_missing_file_returns_empty(self, tmp_path):
        s = Standardizer(config_dir=tmp_path / "nonexistent")
        assert s.get_country_info() == {}
