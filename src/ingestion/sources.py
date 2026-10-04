"""Детерминированный поток продавца и проверка ответов внешних API."""
import hashlib
import json
import math
import random
from datetime import date, datetime, timedelta, timezone

MSK = timezone(timedelta(hours=3))
WEATHER_FIELDS = (
    "temperature_2m_mean", "temperature_2m_min", "temperature_2m_max",
    "precipitation_sum", "weather_code",
)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def dates(start, end):
    while start <= end:
        yield start
        start += timedelta(days=1)


def synthetic_day(day, seed, sku_count, region):
    """Один неизменяемый дневной пакет; seed не зависит от порядка загрузки."""
    orders, stocks = [], []
    for index in range(1, sku_count + 1):
        sku = f"SKU-{index:03d}"
        key = f"v1:{seed}:{day}:{sku}".encode()
        rng = random.Random(int.from_bytes(hashlib.sha256(key).digest(), "big"))
        category = (index - 1) % 3
        base_price = 300 + index * 75
        promo = int(rng.random() < 0.12)
        discount = rng.choice([10, 15, 20, 25]) if promo else 0
        price = round(base_price * (1 + 0.06 * math.sin(day.toordinal() / 50)), 2)
        annual = 1 + 0.3 * math.cos(2 * math.pi * (day.timetuple().tm_yday - category * 100) / 365.25)
        weekly = 1.3 if day.weekday() >= 5 else 1.0
        anomaly = 2.5 if rng.random() < 0.008 else 1.0
        potential = max(0, round((5 + index % 12) * annual * weekly * (1 + promo * 0.6)
                                 * (base_price / price) * anomaly + rng.gauss(0, 2)))
        opening_stock = 0 if rng.random() < 0.04 else rng.randint(4, 100)
        remaining = opening_stock
        for item in range(potential):
            if remaining == 0:
                break
            qty = min(rng.choice([1, 1, 1, 2]), remaining)
            status = "cancelled" if rng.random() < 0.07 else "completed"
            payment = "unpaid" if rng.random() < 0.04 else "paid"
            if status != "cancelled" and payment == "paid":
                remaining -= qty
            order_id = f"ORD-{day:%Y%m%d}-{index:03d}-{item:04d}"
            orders.append(dict(order_id=order_id, order_item_id="1", sku_id=sku,
                               event_time=f"{day}T{rng.randrange(24):02d}:{rng.randrange(60):02d}:00+03:00",
                               category_id=f"CAT-{category + 1}", quantity=qty, unit_price=price,
                               discount_pct=discount, promo_flag=promo, order_status=status,
                               payment_status=payment, warehouse_id="WH-01", sales_region=region))
        stocks.append(dict(snapshot_date=str(day), sku_id=sku, warehouse_id="WH-01",
                           category_id=f"CAT-{category + 1}", unit_price=price,
                           discount_pct=discount, promo_flag=promo,
                           opening_stock=opening_stock, available_stock=remaining,
                           planned_receipts=rng.choice([0, 0, 20, 50]), sales_region=region))
    validate_synthetic(orders, stocks, day, sku_count)
    return orders, stocks


def validate_synthetic(orders, stocks, day, sku_count):
    if len(stocks) != sku_count or len({s["sku_id"] for s in stocks}) != sku_count:
        raise ValueError("Неверное число SKU в снимке")
    keys = set()
    for row in orders:
        key = row["order_id"], row["order_item_id"]
        if key in keys:
            raise ValueError("Дубли ключей заказов")
        keys.add(key)
        if row["quantity"] <= 0 or row["unit_price"] <= 0 or not 0 <= row["discount_pct"] <= 100:
            raise ValueError("Неверное количество, цена или скидка")
        if row["order_status"] not in {"completed", "cancelled"} or row["payment_status"] not in {"paid", "unpaid"}:
            raise ValueError("Неизвестный статус")
        if datetime.fromisoformat(row["event_time"]).astimezone(MSK).date() != day:
            raise ValueError("Событие вне дневного пакета")
    for row in stocks:
        if min(row["opening_stock"], row["available_stock"], row["planned_receipts"]) < 0:
            raise ValueError("Отрицательный остаток")


def parse_calendar(payload, year):
    days = list(dates(date(year, 1, 1), date(year, 12, 31)))
    text = payload.decode("utf-8").strip()
    # pre=1, holiday=1: 0 рабочий, 1 выходной, 2 сокращённый, 4 особый, 8 праздник.
    if len(text) != len(days) or set(text) - set("01248"):
        raise ValueError(f"isDayOff: ожидалось {len(days)} кодов, получено {len(text)}; ответ: {text[:60]}")
    return [dict(date=str(day), day_code=int(code), is_day_off=int(code in "148"),
                 is_short_day=int(code == "2"), is_holiday=int(code == "8"), weekday=day.weekday())
            for day, code in zip(days, text)]


def parse_weather(payload, start, end, location, fetched_at):
    body = json.loads(payload)
    daily, units = body["daily"], body["daily_units"]
    expected = [str(day) for day in dates(start, end)]
    if daily["time"] != expected:
        raise ValueError("Open-Meteo: неполный или неверный диапазон дат")
    for field in WEATHER_FIELDS:
        if len(daily[field]) != len(expected):
            raise ValueError(f"Open-Meteo: неверная длина {field}")
        expected_unit = "°C" if field.startswith("temperature") else "mm" if field == "precipitation_sum" else "wmo code"
        if units[field] != expected_unit:
            raise ValueError(f"Open-Meteo: неверная единица {field}: {units[field]}")
    rows = []
    for i, day in enumerate(expected):
        values = {field: daily[field][i] for field in WEATHER_FIELDS}
        if any(v is None or not isinstance(v, (int, float)) or not math.isfinite(v) for v in values.values()):
            raise ValueError(f"Open-Meteo: пропуск/нечисловое значение за {day}; повторите загрузку позже")
        if values["precipitation_sum"] < 0 or not values["temperature_2m_min"] <= values["temperature_2m_mean"] <= values["temperature_2m_max"]:
            raise ValueError(f"Open-Meteo: неверная температура/осадки за {day}")
        if values["weather_code"] not in {0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99}:
            raise ValueError("Open-Meteo: неизвестный код WMO")
        rows.append(dict(date=day, location=location, fetched_at=fetched_at, **values))
    return rows
