#!/usr/bin/env python3
import os
import sys
import time
import math
import csv
import json
import argparse
from typing import Dict, Any, List, Tuple, Set, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


YANDEX_SEARCH_API_URL = "https://search-maps.yandex.ru/v1/"

# Preset bounding boxes for CIS countries (min_lon, min_lat, max_lon, max_lat)
COUNTRY_BBOXES: Dict[str, Tuple[float, float, float, float]] = {
    # ISO-like short codes
    "KG": (69.2, 39.2, 80.35, 43.35),            # Kyrgyzstan
    "KZ": (46.5, 40.56, 87.3, 55.6),             # Kazakhstan
    "RU": (19.6, 41.2, 190.0, 82.2),             # Russia (crosses 180th meridian; simplified)
    "BY": (23.1, 51.2, 32.8, 56.2),              # Belarus
    "UA": (22.1, 44.0, 40.3, 52.5),              # Ukraine
    "UZ": (55.9, 37.2, 73.4, 46.8),              # Uzbekistan
    "TJ": (67.3, 36.6, 75.2, 41.1),              # Tajikistan
    "TM": (52.4, 35.1, 66.7, 42.8),              # Turkmenistan
    "AM": (43.4, 38.8, 46.7, 41.3),              # Armenia
    "AZ": (44.8, 38.4, 51.9, 41.9),              # Azerbaijan
    "GE": (39.9, 41.0, 46.8, 43.7),              # Georgia
}

# Human-friendly country names for interactive menu
COUNTRY_LABELS: Dict[str, str] = {
    "KG": "Кыргызстан",
    "KZ": "Казахстан",
    "RU": "Россия",
    "BY": "Беларусь",
    "UA": "Украина",
    "UZ": "Узбекистан",
    "TJ": "Таджикистан",
    "TM": "Туркменистан",
    "AM": "Армения",
    "AZ": "Азербайджан",
    "GE": "Грузия",
}


def load_env_if_available() -> None:
    if load_dotenv is not None:
        load_dotenv()


def create_session(max_retries: int = 5, backoff_factor: float = 0.5) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        status=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "kg-schools-collector/1.0 (+https://example.local)"
    })
    return session


def country_bbox(country_code: str) -> Tuple[float, float, float, float]:
    code = country_code.upper()
    if code not in COUNTRY_BBOXES:
        raise ValueError(
            f"Unsupported country code '{country_code}'. Supported: {', '.join(sorted(COUNTRY_BBOXES.keys()))}"
        )
    return COUNTRY_BBOXES[code]


def generate_tiles(
    bbox: Tuple[float, float, float, float],
    tiles_per_axis: int,
) -> List[Tuple[float, float, float, float]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    lon_step = (max_lon - min_lon) / tiles_per_axis
    lat_step = (max_lat - min_lat) / tiles_per_axis
    tiles: List[Tuple[float, float, float, float]] = []
    for i in range(tiles_per_axis):
        for j in range(tiles_per_axis):
            left = min_lon + j * lon_step
            right = left + lon_step
            bottom = min_lat + i * lat_step
            top = bottom + lat_step
            tiles.append((left, bottom, right, top))
    return tiles


def bbox_to_param(tile: Tuple[float, float, float, float]) -> str:
    left, bottom, right, top = tile
    # Yandex expects bbox as "lon1,lat1~lon2,lat2"
    return f"{left},{bottom}~{right},{top}"


def fetch_tile(
    session: requests.Session,
    api_key: str,
    tile: Tuple[float, float, float, float],
    text_query: str = "школа",
    lang: str = "ru_RU",
    results_per_page: int = 50,
    max_pages: int = 20,
    rate_limit_sleep_s: float = 0.25,
) -> List[Dict[str, Any]]:
    """Fetch all results from a tile using pagination via results+skip.

    Yandex Search API parameters:
    - text: query string
    - type: biz (organizations)
    - bbox: lon1,lat1~lon2,lat2
    - lang: e.g., ru_RU
    - results: items per page (max 50)
    - skip: offset
    """
    all_features: List[Dict[str, Any]] = []
    for page in range(max_pages):
        skip = page * results_per_page
        params = {
            "text": text_query,
            "type": "biz",
            "bbox": bbox_to_param(tile),
            "lang": lang,
            "results": results_per_page,
            "skip": skip,
            "apikey": api_key,
        }
        resp = session.get(YANDEX_SEARCH_API_URL, params=params, timeout=30)
        # Respect server-side rate limits
        if resp.status_code == 429:
            time.sleep(1.5)
            resp = session.get(YANDEX_SEARCH_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        # Heuristic: if fewer than requested returned, likely last page
        if len(features) < results_per_page:
            break
        time.sleep(rate_limit_sleep_s)
    return all_features


def normalize_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
    properties = feature.get("properties", {})
    company = properties.get("CompanyMetaData", {})
    geometry = feature.get("geometry", {})
    coords = geometry.get("coordinates") or [None, None]
    phones = company.get("Phones") or []
    categories = company.get("Categories") or []
    hours = company.get("Hours") or {}
    links = company.get("Links") or []

    def first_or_none(items: List[Dict[str, Any]], key: str) -> Any:
        if not isinstance(items, list) or not items:
            return None
        return items[0].get(key)

    phone_numbers: List[str] = []
    for p in phones:
        num = p.get("formatted") or p.get("number") or p.get("value")
        if num:
            phone_numbers.append(num)

    categories_list: List[str] = []
    for c in categories:
        n = c.get("name") or c.get("class")
        if n:
            categories_list.append(n)

    site_url = first_or_none(links, "href")

    normalized = {
        "id": properties.get("id") or company.get("id"),
        "name": company.get("name") or properties.get("name"),
        "address": company.get("address") or properties.get("description"),
        "lat": coords[1] if isinstance(coords, list) and len(coords) == 2 else None,
        "lon": coords[0] if isinstance(coords, list) and len(coords) == 2 else None,
        "phones": ", ".join(phone_numbers) if phone_numbers else None,
        "categories": ", ".join(categories_list) if categories_list else None,
        "categories_list": categories_list,
        "hours_text": (hours.get("text") if isinstance(hours, dict) else None),
        "site_url": site_url,
        "raw": feature,
    }
    return normalized


def is_kyrgyz_school(item: Dict[str, Any]) -> bool:
    """Heuristic filter to keep only general education schools in Kyrgyzstan.

    - Category must include typical school types (e.g., "Школа", "Гимназия", "Лицей").
    - Exclude training centers like auto/martial arts/music/language/sports/etc.
    - Address should reference Kyrgyzstan if present; otherwise rely on bbox guard.
    """
    allowed_category_names = {
        "Школа",
        "Средняя школа",
        "Гимназия",
        "Лицей",
        "Школа-интернат",
        "Общеобразовательная школа",
    }

    categories_list = [c.strip() for c in (item.get("categories_list") or [])]
    categories_lower = ", ".join(categories_list).lower()
    name_lower = (item.get("name") or "").lower()

    # Exclusion substrings (training/extra-curricular/other types)
    exclude_markers = [
        "автошкол", "вождени", "driv",  # driving schools
        "танц", "dance", "хореограф",    # dance
        "музык", "music",                 # music
        "язы", "language",                # language
        "искусств", "art",                # arts
        "спорт", "фитнес", "йога",      # sports/fitness/yoga
        "карат", "таэквондо", "айкидо",  # martial arts
        "футбол", "теннис", "баскетбол", # sports
        "сад", "детсад", "детский сад",  # kindergarten
        "it ", " it", "айти", "программир", # IT courses
        "колледж", "университет", "институт", # higher ed
        "школа искусств", "дши",          # arts school
    ]

    # If any exclusion marker appears in categories or name, drop
    for marker in exclude_markers:
        if marker in categories_lower or marker in name_lower:
            return False

    # If we have explicit category names, keep when they intersect allowed
    if categories_list:
        if any(cat in allowed_category_names for cat in categories_list):
            pass
        else:
            # Keep if the generic word "Школа" appears without exclusions
            if "школ" not in categories_lower:
                return False

    # Country/address heuristic
    address = (item.get("address") or "").lower()
    if address:
        if not ("кыргызстан" in address or "kyrgyzstan" in address or "кыргызская республика" in address):
            # If address present but doesn't mention KG, still allow because many addresses omit country.
            # The bbox already restricts to KG vicinity; to be strict, uncomment the next line to filter out.
            # return False
            pass

    return True


def dedupe(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def norm_text(value: Any) -> str:
        if not value:
            return ""
        s = str(value).strip().lower()
        # Basic normalization: collapse spaces and remove punctuation that often varies
        for ch in [",", ".", "\n", "\t", "\r", ";", ":"]:
            s = s.replace(ch, " ")
        s = " ".join(s.split())
        return s

    seen: Set[str] = set()
    result: List[Dict[str, Any]] = []
    for f in features:
        fid = f.get("id")

        name_key = norm_text(f.get("name"))
        addr_key = norm_text(f.get("address"))
        lat = f.get("lat")
        lon = f.get("lon")
        # Round coordinates to ~20 meters (5 decimals ~ 1.1m, use 4 decimals ~ 11m)
        lat_key = f"{round(lat, 4) if isinstance(lat, (int, float)) else ''}"
        lon_key = f"{round(lon, 4) if isinstance(lon, (int, float)) else ''}"

        composite_keys = [
            fid or "",
            f"name_addr::{name_key}|{addr_key}",
            f"name_coord::{name_key}|{lat_key},{lon_key}",
        ]

        chosen_key = None
        for k in composite_keys:
            if k:
                if k in seen:
                    chosen_key = k
                    break
        if chosen_key is not None:
            continue

        # Register all non-empty keys to the seen set to suppress close duplicates
        for k in composite_keys:
            if k:
                seen.add(k)
        result.append(f)
    return result


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="get_kg_schools",
        description="Collects school organizations from Yandex Maps Search API for selected CIS country.",
    )
    parser.add_argument(
        "--country",
        default=os.getenv("COUNTRY", "KG"),
        help=f"Country code to search (supported: {', '.join(sorted(COUNTRY_BBOXES.keys()))}). Default: %(default)s",
    )
    parser.add_argument(
        "--tiles",
        type=int,
        default=int(os.getenv("TILES_PER_AXIS", "8")),
        help="Tiles per axis (higher => more requests, more coverage). Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", os.path.join(os.getcwd(), "output")),
        help="Directory to save CSV/JSON. Default: %(default)s",
    )
    parser.add_argument(
        "--strict",
        dest="strict",
        action="store_true",
        default=(os.getenv("STRICT_SCHOOL_FILTER", "1") != "0"),
        help="Enable strict filtering for general education schools (default on)",
    )
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Disable strict school filtering",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=os.getenv("YANDEX_MAPS_API_KEY"),
        help="Yandex Maps Search API key. If omitted, uses YANDEX_MAPS_API_KEY env or .env",
    )
    parser.add_argument(
        "--lang",
        default=os.getenv("YANDEX_LANG", "ru_RU"),
        help="Language for results (e.g., ru_RU). Default: %(default)s",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Run in interactive mode (menu to choose country and options)",
    )
    return parser.parse_args(argv)


def save_csv(path: str, items: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "id",
        "name",
        "address",
        "lat",
        "lon",
        "phones",
        "categories",
        "hours_text",
        "site_url",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for it in items:
            writer.writerow({k: it.get(k) for k in fieldnames})


def save_json(path: str, items: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def run_collection(country: str, tiles: int, strict: bool, output_dir: str, api_key: str, lang: str) -> Tuple[str, str, int]:
    try:
        bbox = country_bbox(country)
    except ValueError as e:
        raise SystemExit(str(e))

    os.makedirs(output_dir, exist_ok=True)

    session = create_session()
    tiles_list = generate_tiles(bbox, tiles)

    all_features: List[Dict[str, Any]] = []
    for idx, tile in enumerate(tiles_list, start=1):
        try:
            features = fetch_tile(session, api_key, tile, lang=lang)
            normalized = [normalize_feature(ft) for ft in features]
            if strict:
                normalized = [it for it in normalized if is_kyrgyz_school(it)]
            all_features.extend(normalized)
            time.sleep(0.1)
        except requests.HTTPError as e:
            print(f"Tile {idx}/{len(tiles_list)} failed with HTTP error: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Tile {idx}/{len(tiles_list)} failed: {e}", file=sys.stderr)

    deduped = dedupe(all_features)

    country_lower = country.lower()
    csv_path = os.path.join(output_dir, f"{country_lower}_schools.csv")
    json_path = os.path.join(output_dir, f"{country_lower}_schools.json")
    save_csv(csv_path, deduped)
    save_json(json_path, deduped)
    return csv_path, json_path, len(deduped)


def prompt_input(prompt: str, default: Optional[str] = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    value = input(f"{prompt}{suffix}: ").strip()
    if not value and default is not None:
        return default
    return value


def run_interactive(defaults: argparse.Namespace) -> None:
    print("Выберите страну для сбора школ:")
    codes = sorted(COUNTRY_BBOXES.keys())
    for idx, code in enumerate(codes, start=1):
        name = COUNTRY_LABELS.get(code, code)
        print(f"  {idx}. {name} ({code})")

    while True:
        choice = prompt_input("Введите номер страны", "1")
        if choice.isdigit() and 1 <= int(choice) <= len(codes):
            country = codes[int(choice) - 1]
            break
        print("Некорректный выбор, попробуйте снова.")

    tiles_str = prompt_input("Сколько тайлов по оси (рекомендуется 8-12)", str(defaults.tiles))
    try:
        tiles = max(1, int(tiles_str))
    except Exception:
        tiles = defaults.tiles

    strict_answer = prompt_input("Строгая фильтрация только общеобразовательных школ? (y/n)", "y" if defaults.strict else "n").lower()
    strict = strict_answer.startswith("y")

    output_dir = prompt_input("Папка вывода", defaults.output_dir)

    api_key = defaults.api_key or os.getenv("YANDEX_MAPS_API_KEY")
    if not api_key:
        api_key = prompt_input("Введите ваш Yandex Maps API ключ")
        if not api_key:
            print("API ключ обязателен. Завершение.", file=sys.stderr)
            raise SystemExit(1)

    lang = prompt_input("Язык результатов (например, ru_RU)", defaults.lang)

    print("Начинаю сбор... это может занять несколько минут.")
    csv_path, json_path, count = run_collection(country, tiles, strict, output_dir, api_key, lang)
    print(f"Готово! Найдено записей: {count}\n- {csv_path}\n- {json_path}")


def main(argv: Optional[List[str]] = None) -> None:
    load_env_if_available()
    args = parse_args(argv)

    # Default to interactive mode when no CLI options provided
    if args.interactive or len(sys.argv) == 1:
        return run_interactive(args)

    api_key = args.api_key or os.getenv("YANDEX_MAPS_API_KEY")
    if not api_key:
        print("Error: Please provide API key via --api-key or YANDEX_MAPS_API_KEY env.", file=sys.stderr)
        sys.exit(1)

    csv_path, json_path, count = run_collection(
        country=args.country,
        tiles=int(args.tiles),
        strict=bool(args.strict),
        output_dir=args.output_dir,
        api_key=api_key,
        lang=args.lang,
    )
    print(f"Done. Saved {count} records to:\n- {csv_path}\n- {json_path}")


if __name__ == "__main__":
    main()


