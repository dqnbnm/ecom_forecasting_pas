"""CLI получения данных. Python 3.11+, только стандартная библиотека."""
import argparse
import csv
import json
import sqlite3
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

from .http import HttpClient, atomic_write
from .sources import MSK, WEATHER_FIELDS, dates, now, parse_calendar, parse_weather, synthetic_day

ROOT = Path(__file__).resolve().parents[2]


class Store:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                source TEXT NOT NULL, key TEXT NOT NULL, date TEXT NOT NULL,
                sku TEXT, payload TEXT NOT NULL, PRIMARY KEY(source, key));
            CREATE INDEX IF NOT EXISTS records_date ON records(source, date);
            CREATE TABLE IF NOT EXISTS completed_days (date TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, source TEXT NOT NULL,
                started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                requested_start TEXT NOT NULL, requested_end TEXT NOT NULL,
                status TEXT NOT NULL, received INTEGER NOT NULL, inserted INTEGER NOT NULL,
                updated INTEGER NOT NULL, unchanged INTEGER NOT NULL,
                details TEXT NOT NULL, error TEXT);
        """)

    def put(self, source, key, row, stats):
        payload = json.dumps(row, ensure_ascii=False, sort_keys=True)
        existing = self.db.execute("SELECT payload FROM records WHERE source=? AND key=?", (source, key)).fetchone()
        stats["received"] += 1
        if existing and existing[0] == payload:
            stats["unchanged"] += 1
            return
        self.db.execute("INSERT INTO records VALUES (?,?,?,?,?) ON CONFLICT(source,key) DO UPDATE SET payload=excluded.payload",
                        (source, key, row.get("date", row.get("snapshot_date", row.get("event_time", "")[:10])), row.get("sku_id"), payload))
        stats["updated" if existing else "inserted"] += 1

    def rows(self, source):
        for row in self.db.execute("SELECT payload FROM records WHERE source=? ORDER BY date,key", (source,)):
            yield json.loads(row[0])

    def available_dates(self, source):
        return {row[0] for row in self.db.execute("SELECT DISTINCT date FROM records WHERE source=?", (source,))}

    def counts(self):
        return {row[0]: row[1] for row in self.db.execute("SELECT source,count(*) FROM records GROUP BY source")}

    def check_settings(self, config):
        # Запрещаем смешать другой seed, регион или количество SKU с прежней БД.
        signature = json.dumps({k: config[k] for k in ("seed", "sku_count", "location", "timezone")}, sort_keys=True, ensure_ascii=False)
        previous = self.db.execute("SELECT value FROM settings WHERE key='source_signature'").fetchone()
        if previous and previous[0] != signature:
            raise ValueError("Параметры источников изменились. Для новой конфигурации используйте другую --data-dir.")
        self.db.execute("INSERT OR IGNORE INTO settings VALUES ('source_signature',?)", (signature,))
        self.db.commit()


def missing_ranges(start, end, present, chunk_days=366):
    """Группируем только отсутствующие дни, не скрывая дыр внутри истории."""
    first, last = None, None
    for day in dates(start, end):
        if str(day) not in present:
            if first is None:
                first = day
            last = day
            if (last - first).days + 1 == chunk_days:
                yield first, last
                first, last = None, None
        elif first is not None:
            yield first, last
            first, last = None, None
    if first is not None:
        yield first, last


def write_jsonl(path, rows):
    content = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows).encode()
    atomic_write(path, content)


def load_synthetic(store, client, config, start, end, stats):
    present = {row[0] for row in store.db.execute("SELECT date FROM completed_days")}
    for day in dates(start, end):
        if str(day) in present:
            stats["skipped_days"] += 1
            continue
        orders, stocks = synthetic_day(day, config["seed"], config["sku_count"], config["location"]["name"])
        folder = client.root / "synthetic" / str(day)
        write_jsonl(folder / "orders.jsonl", orders)
        write_jsonl(folder / "stocks.jsonl", stocks)
        for row in orders:
            store.put("orders", row["order_id"] + ":" + row["order_item_id"], row, stats)
        for row in stocks:
            store.put("stocks", f"{day}:{row['warehouse_id']}:{row['sku_id']}", row, stats)
        store.db.execute("INSERT INTO completed_days VALUES (?)", (str(day),))


def load_calendar(store, client, config, start, end, stats):
    # Календарь заранее включает горизонт прогноза на текущую дату.
    today = datetime.now(MSK).date()
    years = set(range(start.year, end.year + 1)) | {today.year, (today + timedelta(days=6)).year}
    for year in sorted(years):
        url = "https://isdayoff.ru/api/getdata?" + urlencode(dict(year=year, cc="ru", pre=1, holiday=1))
        payload, fetched = client.get("isdayoff", url, config["calendar_cache_days"] * 86400)
        rows = parse_calendar(payload, year)
        for row in rows:
            # Timestamp сохраняется в manifest; повтор получения не меняет строку календаря.
            store.put("calendar", row["date"], row, stats)


def weather_url(config, endpoint, start, end):
    location = config["location"]
    params = dict(latitude=location["latitude"], longitude=location["longitude"],
                  start_date=str(start), end_date=str(end), timezone=config["timezone"],
                  daily=",".join(WEATHER_FIELDS), temperature_unit="celsius", precipitation_unit="mm")
    if endpoint == "archive":
        params["models"] = "era5"
    base = "https://archive-api.open-meteo.com/v1/archive" if endpoint == "archive" else "https://api.open-meteo.com/v1/forecast"
    return base + "?" + urlencode(params)


def load_history(store, client, config, start, end, stats):
    # ERA5 публикуется с задержкой; не подставляем недоступные последние дни.
    safe_end = datetime.now(MSK).date() - timedelta(days=6)
    if end > safe_end:
        raise ValueError(f"История ERA5 доступна с задержкой. Укажите --end не позже {safe_end}.")
    present = set() if client.refresh else store.available_dates("weather_history")
    for first, last in missing_ranges(start, end, present):
        payload, fetched = client.get("open_meteo_history", weather_url(config, "archive", first, last))
        for row in parse_weather(payload, first, last, config["location"]["name"], fetched):
            # В истории дата получения хранится в метаданных исходного ответа.
            row.pop("fetched_at")
            store.put("weather_history", row["date"] + ":" + row["location"], row, stats)
    stats["skipped_days"] = sum(str(day) in present for day in dates(start, end))


def load_forecast(store, client, config, start, end, stats):
    today = datetime.now(MSK).date()
    last = today + timedelta(days=6)
    payload, fetched = client.get("open_meteo_forecast", weather_url(config, "forecast", today, last),
                                  config["forecast_cache_minutes"] * 60)
    for row in parse_weather(payload, today, last, config["location"]["name"], fetched):
        store.put("weather_forecast", f"{fetched}:{row['date']}:{row['location']}", row, stats)
    stats["forecast_start"], stats["forecast_end"] = str(today), str(last)
    stats["forecast_fetched_at"] = fetched


LOADERS = {"synthetic": load_synthetic, "calendar": load_calendar,
           "weather_history": load_history, "weather_forecast": load_forecast}


def export_csv(path, rows):
    rows = iter(rows)
    first = next(rows, None)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        if first is not None:
            writer = csv.DictWriter(stream, fieldnames=list(first))
            writer.writeheader()
            writer.writerow(first)
            writer.writerows(rows)


def demand_rows(store):
    totals = {}
    for order in store.rows("orders"):
        if order["payment_status"] == "paid" and order["order_status"] != "cancelled":
            key = order["event_time"][:10], order["sku_id"]
            totals[key] = totals.get(key, 0) + order["quantity"]
    for stock in store.rows("stocks"):
        day, sku = stock["snapshot_date"], stock["sku_id"]
        yield dict(date=day, sku_id=sku, demand_qty=totals.get((day, sku), 0),
                   opening_stock=stock["opening_stock"], available_stock=stock["available_stock"],
                   stockout_flag=int(stock["available_stock"] == 0),
                   planned_receipts=stock["planned_receipts"], unit_price=stock["unit_price"],
                   discount_pct=stock["discount_pct"], promo_flag=stock["promo_flag"])


def export_all(store, directory):
    for source in ("orders", "stocks", "calendar", "weather_history", "weather_forecast"):
        export_csv(directory / "interim" / f"{source}.csv", store.rows(source))
    export_csv(directory / "processed" / "daily_demand.csv", demand_rows(store))


def run(config, start, end, directory, offline=False, refresh=False, sources=None, raw_dir=None):
    if start > end:
        raise ValueError("Начало периода позже окончания")
    if end >= datetime.now(MSK).date():
        raise ValueError("Заказы загружаются только за завершившиеся дни")
    if config["timezone"] != "Europe/Moscow" or config["sku_count"] <= 0:
        raise ValueError("Первая версия поддерживает Europe/Moscow и положительное число SKU")
    directory = Path(directory).resolve()
    store = Store(directory / "interim" / "sources.sqlite")
    try:
        store.check_settings(config)
        client = HttpClient(Path(raw_dir).resolve() if raw_dir else directory / "raw",
                            config["timeout_seconds"], config["attempts"], offline, refresh)
        batch_id = uuid.uuid4().hex
        results = []
        for source in sources or list(LOADERS):
            started = now()
            stats = dict(received=0, inserted=0, updated=0, unchanged=0, skipped_days=0)
            before = len(client.requests)
            error, status = None, "ok"
            try:
                with store.db:
                    LOADERS[source](store, client, config, start, end, stats)
            except Exception as exception:
                error, status = str(exception), "error"
                # Транзакция источника откатилась целиком.
                stats["inserted"] = stats["updated"] = stats["unchanged"] = 0
            requests = client.requests[before:]
            finished = now()
            details = dict(**stats, requests=requests)
            requested_start = stats.get("forecast_start", str(start))
            requested_end = stats.get("forecast_end", str(end))
            with store.db:
                store.db.execute("INSERT INTO ingestion_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                 (uuid.uuid4().hex, batch_id, source, started, finished, requested_start,
                                  requested_end, status, stats["received"], stats["inserted"], stats["updated"],
                                  stats["unchanged"], json.dumps(details, ensure_ascii=False), error))
            result = dict(source=source, status=status, started_at=started, finished_at=finished,
                          requested_start=requested_start, requested_end=requested_end, **details, error=error)
            results.append(result)
            print(f"{source}: {status}; получено={stats['received']}, новых={stats['inserted']}, "
                  f"обновлено={stats['updated']}, без изменений={stats['unchanged']}, пропущено дней={stats['skipped_days']}")
            if error:
                print(f"  Ошибка: {error}")
        export_all(store, directory)
        export_csv(directory / "interim" / "ingestion_runs.csv",
                   (dict(row) for row in store.db.execute("SELECT * FROM ingestion_runs ORDER BY started_at")))
        summary = dict(batch_id=batch_id, created_at=now(), start=str(start), end=str(end),
                       offline=offline, counts=store.counts(), results=results)
        atomic_write(directory / "interim" / "last_run.json", json.dumps(summary, ensure_ascii=False, indent=2).encode())
        return summary
    finally:
        store.db.close()


def main():
    parser = argparse.ArgumentParser(description="Получение источников данных проекта")
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "sources.json")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--raw-dir", type=Path, help="Каталог исходных ответов для воспроизведения в отдельной БД")
    parser.add_argument("--offline", action="store_true", help="Только ранее сохранённые ответы API")
    parser.add_argument("--refresh", action="store_true", help="Повторно запросить внешние данные")
    parser.add_argument("--sources", nargs="+", choices=list(LOADERS))
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    try:
        summary = run(config, args.start or date.fromisoformat(config["history_start"]),
                      args.end or date.fromisoformat(config["history_end"]), args.data_dir,
                      args.offline, args.refresh, args.sources, args.raw_dir)
    except (ValueError, OSError) as error:
        parser.exit(1, f"Ошибка запуска: {error}\n")
    sys.exit(1 if any(r["status"] != "ok" for r in summary["results"]) else 0)


if __name__ == "__main__":
    main()
