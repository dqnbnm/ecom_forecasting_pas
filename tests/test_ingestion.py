import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.http import HttpClient
from src.ingestion.pipeline import Store, missing_ranges, run
from src.ingestion.sources import parse_calendar, parse_weather, synthetic_day


class IngestionTests(unittest.TestCase):
    def test_daily_packets_are_independent_and_reproducible(self):
        day = date(2025, 1, 1)
        first = synthetic_day(day, 42, 30, "Москва")
        synthetic_day(date(2025, 1, 2), 42, 30, "Москва")
        self.assertEqual(first, synthetic_day(day, 42, 30, "Москва"))
        self.assertNotEqual(first, synthetic_day(day, 43, 30, "Москва"))
        for stock in first[1]:
            sold = sum(o["quantity"] for o in first[0] if o["sku_id"] == stock["sku_id"]
                       and o["payment_status"] == "paid" and o["order_status"] != "cancelled")
            self.assertEqual(stock["opening_stock"] - sold, stock["available_stock"])

    def test_calendar_leap_year_and_special_codes(self):
        rows = parse_calendar(b"12840" + b"0" * 361, 2024)
        self.assertEqual(len(rows), 366)
        self.assertEqual([r["is_day_off"] for r in rows[:5]], [1, 0, 1, 1, 0])
        self.assertEqual(rows[1]["is_short_day"], 1)
        self.assertEqual(rows[2]["is_holiday"], 1)
        for bad in (b"100", b"x" * 366):
            with self.assertRaises(ValueError):
                parse_calendar(bad, 2024)

    def test_weather_rejects_missing_values_and_wrong_units(self):
        body = dict(daily=dict(time=["2025-01-01"], temperature_2m_mean=[0], temperature_2m_min=[-2],
                               temperature_2m_max=[2], precipitation_sum=[0], weather_code=[0]),
                    daily_units=dict(temperature_2m_mean="°C", temperature_2m_min="°C", temperature_2m_max="°C",
                                     precipitation_sum="mm", weather_code="wmo code"))
        parse = lambda: parse_weather(json.dumps(body).encode(), date(2025, 1, 1), date(2025, 1, 1), "Москва", "timestamp")
        self.assertEqual(parse()[0]["temperature_2m_mean"], 0)
        body["daily"]["temperature_2m_mean"] = [None]
        with self.assertRaises(ValueError):
            parse()
        body["daily"]["temperature_2m_mean"] = [0]
        body["daily_units"]["precipitation_sum"] = "inch"
        with self.assertRaises(ValueError):
            parse()

    def test_missing_ranges_fill_internal_gaps(self):
        ranges = list(missing_ranges(date(2025, 1, 1), date(2025, 1, 7), {"2025-01-01", "2025-01-04"}, 2))
        self.assertEqual(ranges, [(date(2025, 1, 2), date(2025, 1, 3)),
                                  (date(2025, 1, 5), date(2025, 1, 6)), (date(2025, 1, 7), date(2025, 1, 7))])

    def test_pipeline_repeat_increment_and_failure_journal(self):
        config = dict(seed=42, sku_count=3, location=dict(name="Москва", latitude=55.75, longitude=37.62),
                      timezone="Europe/Moscow", timeout_seconds=1, attempts=1, calendar_cache_days=7)
        with tempfile.TemporaryDirectory() as folder:
            first = run(config, date(2025, 1, 1), date(2025, 1, 2), folder, sources=["synthetic"])
            second = run(config, date(2025, 1, 1), date(2025, 1, 2), folder, sources=["synthetic"])
            third = run(config, date(2025, 1, 1), date(2025, 1, 3), folder, sources=["synthetic"])
            self.assertEqual(first["counts"], second["counts"])
            self.assertEqual(second["results"][0]["inserted"], 0)
            self.assertEqual(third["counts"]["stocks"] - second["counts"]["stocks"], 3)
            failed = run(config, date(2025, 1, 1), date(2025, 1, 3), folder, offline=True, sources=["calendar"])
            self.assertEqual(failed["results"][0]["status"], "error")
            store = Store(Path(folder) / "interim" / "sources.sqlite")
            self.assertEqual(store.db.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0], 4)
            self.assertNotIn("calendar", store.counts())
            changed = dict(config, seed=43)
            with self.assertRaises(ValueError):
                store.check_settings(changed)
            store.db.close()

    def test_http_retries_and_offline_does_not_use_network(self):
        from urllib.error import URLError
        with tempfile.TemporaryDirectory() as folder, patch("urllib.request.urlopen", side_effect=URLError("unavailable")) as request, patch("time.sleep"):
            client = HttpClient(folder, attempts=3)
            with self.assertRaises(RuntimeError):
                client.get("source", "https://example.com")
            self.assertEqual(request.call_count, 3)
            offline = HttpClient(folder, offline=True)
            with self.assertRaises(RuntimeError):
                offline.get("source", "https://example.com")
            self.assertEqual(request.call_count, 3)

    def test_http_preserves_versions_and_reuses_cache(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.headers.get.return_value = "application/json"
        response.read.side_effect = [b'first response', b'second response']
        with tempfile.TemporaryDirectory() as folder, patch("urllib.request.urlopen", return_value=response) as request:
            client = HttpClient(folder)
            first, first_time = client.get("forecast", "https://example.com")
            cached, cached_time = client.get("forecast", "https://example.com")
            self.assertEqual((cached, cached_time), (first, first_time))
            self.assertEqual(request.call_count, 1)
            fresh = HttpClient(folder, refresh=True)
            second, second_time = fresh.get("forecast", "https://example.com")
            self.assertNotEqual(first_time, second_time)
            self.assertEqual(second, b'second response')
            snapshots = list((Path(folder) / "forecast" / "snapshots").rglob("*.response"))
            self.assertEqual({p.read_bytes() for p in snapshots}, {first, second})
            offline = HttpClient(folder, offline=True)
            self.assertEqual(offline.get("forecast", "https://example.com")[0], second)
            self.assertEqual(request.call_count, 2)

    def test_source_failure_rolls_back_partial_rows(self):
        config = dict(seed=42, sku_count=3, location=dict(name="Москва", latitude=55.75, longitude=37.62),
                      timezone="Europe/Moscow", timeout_seconds=1, attempts=1)

        def broken(store, client, config, start, end, stats):
            store.put("stocks", "partial", dict(snapshot_date=str(start), sku_id="SKU-001"), stats)
            store.db.execute("INSERT INTO completed_days VALUES (?)", (str(start),))
            raise ValueError("bad packet")

        with tempfile.TemporaryDirectory() as folder, patch.dict("src.ingestion.pipeline.LOADERS", {"synthetic": broken}):
            summary = run(config, date(2025, 1, 1), date(2025, 1, 1), folder, sources=["synthetic"])
            self.assertEqual(summary["results"][0]["status"], "error")
            self.assertEqual(summary["counts"], {})
            store = Store(Path(folder) / "interim" / "sources.sqlite")
            self.assertEqual(store.db.execute("SELECT count(*) FROM completed_days").fetchone()[0], 0)
            store.db.close()


if __name__ == "__main__":
    unittest.main()
