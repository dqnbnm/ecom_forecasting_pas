"""Проверка загрузки: первый запуск, повтор и добавление дня."""
import argparse
import html
import json
from datetime import date, timedelta
from pathlib import Path

from .http import atomic_write
from .pipeline import ROOT, Store, run


def table(headers, rows):
    esc = lambda value: html.escape(str(value))
    return '<table><thead><tr>' + ''.join(f'<th>{esc(x)}</th>' for x in headers) + '</tr></thead><tbody>' + ''.join(
        '<tr>' + ''.join(f'<td>{esc(value)}</td>' for value in row) + '</tr>' for row in rows) + '</tbody></table>'


def render_report(config, summaries, checks, directory):
    stages = ["1. Загрузка истории", "2. Повтор того же периода", "3. Добавление следующего дня"]
    details = []
    for stage, summary in zip(stages, summaries):
        details.append(f'<h2>{stage}</h2>' + table(
            ["Источник", "Статус", "Получено", "Добавлено", "Изменено", "Без изменений", "Дней пропущено", "HTTP / кэш"],
            [(r["source"], r["status"], r["received"], r["inserted"], r["updated"], r["unchanged"], r["skipped_days"],
              f"{sum(q['origin'] == 'http' for q in r['requests'])} / {sum(q['origin'] == 'cache' for q in r['requests'])}")
             for r in summary["results"]]))
        for result in summary["results"]:
            if result["error"]:
                details.append('<p class="error">' + html.escape(result["error"]) + '</p>')
    store = Store(directory / "interim" / "sources.sqlite")
    try:
        for source in ("orders", "stocks", "calendar", "weather_history", "weather_forecast"):
            samples = []
            for i, row in enumerate(store.rows(source)):
                if i == 3:
                    break
                samples.append(row)
            details.append(f'<h2>Пример: {source}</h2><pre>' + html.escape(json.dumps(samples, ensure_ascii=False, indent=2)) + '</pre>')
    finally:
        store.db.close()
    requests = [q for summary in summaries for r in summary["results"] for q in r["requests"]]
    details.append('<h2>Запросы к API</h2>' + table(
        ["Способ", "Время получения UTC", "Адрес запроса"],
        [(q["origin"], q.get("fetched_at", "—"), q["url"]) for q in requests]))
    check_table = table(["Проверка", "Результат"], [(k, "Пройдена" if v else "НЕ ПРОЙДЕНА") for k, v in checks.items()])
    passed = all(checks.values())
    title = "Подключение источников данных"
    status = "Все проверки пройдены" if passed else "Некоторые проверки не пройдены"
    return f'''<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>
body{{font:16px/1.5 Arial,sans-serif;background:white;color:#222;margin:0}}
main{{max-width:1100px;margin:30px auto;padding:0 24px}}
h1{{font-size:28px}}h2{{font-size:21px;margin-top:28px}}
.status{{font-weight:bold;color:{'#25602d' if passed else '#9b3426'}}}
table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{border:1px solid #ccc;padding:8px;text-align:left;overflow-wrap:anywhere}}
th{{background:#f1f1f1}}pre{{background:#f5f5f5;padding:14px;overflow:auto;font-size:13px}}
.error{{color:#9b3426}}a{{color:#235ca0}}@media(max-width:800px){{main{{margin:16px 0;padding:0 12px}}table{{display:block;overflow:auto}}}}
</style><main><p>Прогнозно-аналитические системы. Второй этап проекта.</p><h1>{title}</h1>
<p>Прогноз спроса на 30 товарных позиций маркетплейса. Регион: {html.escape(config['location']['name'])}.</p>
<p class="status">{status}. Отчёт сформирован {html.escape(summaries[-1]['created_at'])}.</p>
<p>История: {summaries[0]['start']} — {summaries[0]['end']}. Дополнительный день: {summaries[-1]['end']}.
Заказы и остатки синтетические; календарь и погода получены из внешних API. Режим: {'сохранённые ответы (офлайн)' if summaries[0]['offline'] else 'сеть и кэш'}.</p>
<p>Исходные файлы сохраняются в data/raw, затем данные проверяются и записываются в SQLite. Для просмотра создаются CSV и журнал загрузок.</p>
<h2>Проверки</h2>{check_table}
{''.join(details)}
<h2>Примечания</h2><p>Заказы синтетические, поэтому по ним нельзя оценить точность на реальном маркетплейсе.
Историческая погода ERA5 публикуется с задержкой. Прогноз хранится отдельно с временем получения: его нельзя считать известным до этого момента.
Нулевой остаток отмечается, так как он мог ограничить продажи. Построение модели будет на следующем этапе.</p>
<p>Источники: <a href="https://www.isdayoff.ru/docs/">isDayOff</a>,
<a href="https://open-meteo.com/en/docs/historical-weather-api">Open-Meteo Historical Weather</a>,
<a href="https://open-meteo.com/en/docs">Open-Meteo Forecast</a>. Погодные данные: Open-Meteo, ERA5 / Copernicus C3S, CC BY 4.0.</p></main></html>'''


def main():
    parser = argparse.ArgumentParser(description="Демонстрация получения источников: история, повтор, новый день")
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "sources.json")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--report-dir", type=Path, default=ROOT / "reports" / "generated")
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    start, end = date.fromisoformat(config["history_start"]), date.fromisoformat(config["history_end"])
    directory = args.data_dir.resolve()
    summaries = []
    for label, last in (("История", end), ("Повтор", end), ("Новый день", end + timedelta(days=1))):
        print(f"\n=== {label}: {start} — {last} ===")
        summaries.append(run(config, start, last, directory, offline=args.offline, raw_dir=args.raw_dir))
    first, repeat, increment = summaries
    repeat_stable = all(first["counts"].get(s, 0) == repeat["counts"].get(s, 0)
                        for s in ("orders", "stocks", "calendar", "weather_history"))
    expected_days = (end - start).days + 2
    store = Store(directory / "interim" / "sources.sqlite")
    try:
        stock_days = store.available_dates("stocks")
        weather_days = store.available_dates("weather_history")
        calendar_days = store.available_dates("calendar")
        expected = {str(start + timedelta(days=i)) for i in range(expected_days)}
        daily_skus = store.db.execute("SELECT date,count(DISTINCT sku) FROM records WHERE source='stocks' AND date BETWEEN ? AND ? GROUP BY date",
                                     (str(start), str(end + timedelta(days=1)))).fetchall()
    finally:
        store.db.close()
    checks = {
        "Все источники успешно загружены в трёх запусках": all(r["status"] == "ok" for s in summaries for r in s["results"]),
        "Повтор не увеличивает исторические таблицы": repeat_stable,
        "Повтор не добавляет и не изменяет заказы и остатки": repeat["results"][0]["inserted"] == repeat["results"][0]["updated"] == 0,
        "История заказов покрывает не менее двух лет": expected_days >= 730,
        "На каждый день есть снимки всех SKU": len(daily_skus) == expected_days and all(r[1] == config["sku_count"] for r in daily_skus),
        "Все даты покрыты снимками, календарём и погодой": expected <= stock_days & weather_days & calendar_days,
        "После третьего запуска есть следующий день": str(end + timedelta(days=1)) in stock_days & weather_days,
        "Сохранён семидневный прогноз погоды": increment["counts"].get("weather_forecast", 0) >= 7,
    }
    # При повторе всего сценария дополнительный день уже может быть в базе.
    added = increment["counts"].get("stocks", 0) - repeat["counts"].get("stocks", 0)
    checks["Инкремент добавляет один день или пропускает уже загруженный"] = added in (0, config["sku_count"])
    evidence = dict(config=config, stages=summaries, checks=checks)
    atomic_write(args.report_dir / "data_sources_demo.json", json.dumps(evidence, ensure_ascii=False, indent=2).encode())
    atomic_write(args.report_dir / "data_sources_demo.html", render_report(config, summaries, checks, directory).encode())
    print(f"\nОтчёт: {(args.report_dir / 'data_sources_demo.html').resolve()}")
    if not all(checks.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
