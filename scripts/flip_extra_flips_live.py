#!/usr/bin/env python3
"""
Live "extra flips" scan for the current epoch (before validation).

Goal:
- Count how many identities (authors) have published more than N flips (default N=3)
- Compute total extra flips beyond N (sum(max(0, count - N)))
- Optional: fetch stake information:
  - --fetch-stake: fetch stake only for authors with extra flips (flipCount > threshold)
  - --fetch-stake-all: fetch stake for ALL authors who submitted at least 1 flip (heavy)

Endpoints used (Idena indexer API):
- GET /Epoch/Last
- GET /Epoch/{epoch}/Flips (paged)
- GET /Address/{address} (best-effort) OR /Identity/{address} fallback (best-effort)

No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import os
import time
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


BASE_URL_DEFAULT = "https://api.idena.io/api"
MAX_PAGE_SIZE = 100


def utc_ts() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def log(msg: str) -> None:
    print(f"[{utc_ts()}] {msg}", flush=True)


def iso_to_unix(iso_value: str) -> Optional[float]:
    try:
        if iso_value.endswith("Z"):
            iso_value = iso_value[:-1] + "+00:00"
        return datetime.fromisoformat(iso_value).timestamp()
    except Exception:
        return None


def unix_to_iso(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).isoformat(timespec="seconds") + "Z"


def load_json_file(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def write_json_file(path: str, data: Dict[str, Any]) -> None:
    if not path:
        return
    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)


def normalize_progress_points(rows: Any) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = row.get("timestamp")
        if not isinstance(ts, str):
            continue
        normalized.append(
            {
                "timestamp": ts,
                "flipsSeen": int(safe_float(row.get("flipsSeen"))),
                "uniqueAuthors": int(safe_float(row.get("uniqueAuthors"))),
            }
        )
    normalized.sort(key=lambda x: x["timestamp"])
    return normalized


def build_progress_series(history_by_epoch: Dict[str, Any], epoch: int) -> Dict[str, Any]:
    current_points = normalize_progress_points(history_by_epoch.get(str(epoch), []))
    previous_epochs: List[Dict[str, Any]] = []

    for prev_epoch in (epoch - 1, epoch - 2):
        if prev_epoch <= 0:
            continue
        prev_points = normalize_progress_points(history_by_epoch.get(str(prev_epoch), []))
        if prev_points:
            previous_epochs.append({"epoch": prev_epoch, "points": prev_points})

    return {
        "currentEpoch": {"epoch": epoch, "points": current_points},
        "previousEpochs": previous_epochs,
    }


class HttpClient:
    def __init__(self, base_url: str, timeout: int = 25, retries: int = 6, backoff_sec: float = 1.3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff_sec = backoff_sec

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        qs = ""
        if params:
            qs = "?" + urlencode(params)
        url = f"{self.base_url}{path}{qs}"

        last_err: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                req = Request(url, headers={"Accept": "application/json", "User-Agent": "idena-extra-flips-live/1.1"})
                with urlopen(req, timeout=self.timeout) as resp:
                    status = getattr(resp, "status", 200)
                    if status == 429:
                        time.sleep(self.backoff_sec * attempt)
                        continue
                    body = resp.read().decode("utf-8", errors="replace")

                js = json.loads(body)
                err = js.get("error")
                if isinstance(err, dict) and err.get("message"):
                    raise RuntimeError(f"API error at {path}: {err.get('message')}")
                return js
            except (HTTPError, URLError, json.JSONDecodeError, RuntimeError) as e:
                last_err = e
                time.sleep(self.backoff_sec * attempt)

        raise RuntimeError(f"GET failed after {self.retries} retries: {url} ({last_err})")


def extract_epoch_and_validation_time(res: Any) -> Tuple[int, Optional[str]]:
    epoch = 0
    validation_time: Optional[str] = None

    if isinstance(res, int):
        epoch = res
    elif isinstance(res, dict):
        for k in ("epoch", "Epoch", "number", "Number"):
            v = res.get(k)
            if isinstance(v, int):
                epoch = v
                break
        vt = res.get("validationTime")
        if isinstance(vt, str):
            validation_time = vt

    return epoch, validation_time


def get_current_epoch_info(api: HttpClient) -> Tuple[int, Optional[str]]:
    js = api.get_json("/Epoch/Last")
    res = js.get("result")
    epoch, validation_time = extract_epoch_and_validation_time(res)
    if epoch > 0:
        return epoch, validation_time
    raise RuntimeError(f"Unexpected /Epoch/Last result: {res}")


def get_epoch_validation_time(api: HttpClient, epoch: int) -> Optional[str]:
    js = api.get_json(f"/Epoch/{epoch}")
    res = js.get("result")
    _, validation_time = extract_epoch_and_validation_time(res)
    return validation_time


def paged_flips(api: HttpClient, epoch: int, page_size: int, sleep_per_page: float) -> Iterable[Dict[str, Any]]:
    token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"limit": page_size}
        if token:
            params["continuationToken"] = token

        js = api.get_json(f"/Epoch/{epoch}/Flips", params=params)
        items = js.get("result") or []
        if not isinstance(items, list):
            raise RuntimeError(f"Unexpected result type at /Epoch/{epoch}/Flips: {type(items)}")

        for it in items:
            if isinstance(it, dict):
                yield it

        token = js.get("continuationToken")
        if not token:
            break

        if sleep_per_page > 0:
            time.sleep(sleep_per_page)


def paged_bad_authors(api: HttpClient, epoch: int, page_size: int, sleep_per_page: float) -> Iterable[Dict[str, Any]]:
    token: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"limit": page_size}
        if token:
            params["continuationToken"] = token

        js = api.get_json(f"/Epoch/{epoch}/Authors/Bad", params=params)
        items = js.get("result") or []
        if not isinstance(items, list):
            raise RuntimeError(f"Unexpected result type at /Epoch/{epoch}/Authors/Bad: {type(items)}")

        for it in items:
            if isinstance(it, dict):
                yield it

        token = js.get("continuationToken")
        if not token:
            break

        if sleep_per_page > 0:
            time.sleep(sleep_per_page)


def safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def try_get_author(flip: Dict[str, Any]) -> str:
    for k in ("author", "authorAddress", "address"):
        a = flip.get(k)
        if isinstance(a, str) and a.startswith("0x") and len(a) == 42:
            return a.lower()
    return ""


def try_get_cid(flip: Dict[str, Any]) -> str:
    c = flip.get("cid") or flip.get("Cid") or ""
    return c if isinstance(c, str) else ""


def try_get_words(flip: Dict[str, Any]) -> Tuple[str, str]:
    words = flip.get("words")
    if not isinstance(words, dict):
        return "", ""

    word1 = words.get("word1")
    word2 = words.get("word2")
    word1_name = ""
    word2_name = ""

    if isinstance(word1, dict):
        name = word1.get("name")
        if isinstance(name, str):
            word1_name = name

    if isinstance(word2, dict):
        name = word2.get("name")
        if isinstance(name, str):
            word2_name = name

    return word1_name, word2_name


def fetch_wrongwords_bad_authors(api: HttpClient, epoch: int, page_size: int, sleep_per_page: float) -> set[str]:
    bad: set[str] = set()
    for row in paged_bad_authors(api, epoch=epoch, page_size=page_size, sleep_per_page=sleep_per_page):
        addr = row.get("address")
        reason = row.get("reason")
        wrong_words_flag = row.get("wrongWords")

        if isinstance(addr, str) and len(addr) == 42 and addr.startswith("0x"):
            if reason == "WrongWords" or wrong_words_flag is True:
                bad.add(addr.lower())
    return bad


def build_grade_identity_leaderboard(
    api: HttpClient,
    epoch: int,
    page_size: int,
    sleep_per_page: float,
    allowed_statuses: List[str],
    include_zero: bool,
    top_n: int,
    top_flips_n: int,
    out_dir: str,
) -> Dict[str, Any]:
    allowed_status = set(allowed_statuses) if allowed_statuses else None

    log(f"Fetching wrongWords bad authors for epoch {epoch} ...")
    bad_authors = fetch_wrongwords_bad_authors(api, epoch=epoch, page_size=page_size, sleep_per_page=sleep_per_page)
    log(f"wrongWords bad authors (epoch {epoch}): {len(bad_authors)}")

    totals: Dict[str, Dict[str, float]] = {}
    flips_seen = 0
    flips_kept = 0
    flip_heap: List[Tuple[float, str, Dict[str, Any]]] = []
    top_flips_limit = max(0, int(top_flips_n))

    log(f"Building gradeScore identity leaderboard for epoch {epoch} ...")
    for fl in paged_flips(api, epoch=epoch, page_size=page_size, sleep_per_page=sleep_per_page):
        flips_seen += 1
        author = try_get_author(fl)
        if not author or author in bad_authors:
            continue

        status = fl.get("status")
        if allowed_status is not None and status not in allowed_status:
            continue

        grade_score = safe_float(fl.get("gradeScore"))
        if (not include_zero) and grade_score <= 0:
            continue

        flips_kept += 1

        if author not in totals:
            totals[author] = {"total": 0.0, "count": 0.0, "max": 0.0}

        totals[author]["total"] += grade_score
        totals[author]["count"] += 1.0
        if grade_score > totals[author]["max"]:
            totals[author]["max"] = grade_score

        if flips_seen % 2000 == 0:
            log(f"grade leaderboard progress: seen={flips_seen} kept={flips_kept}")

        cid = try_get_cid(fl)
        if top_flips_limit > 0 and cid:
            word1, word2 = try_get_words(fl)
            flip_row = {
                "cid": cid,
                "author": author,
                "gradeScore": grade_score,
                "status": status if isinstance(status, str) else "",
                "word1": word1,
                "word2": word2,
            }
            heap_item = (grade_score, cid, flip_row)
            if len(flip_heap) < top_flips_limit:
                heapq.heappush(flip_heap, heap_item)
            else:
                if (grade_score, cid) > (flip_heap[0][0], flip_heap[0][1]):
                    heapq.heapreplace(flip_heap, heap_item)

    identities = []
    for addr, agg in totals.items():
        count = int(agg["count"])
        total = float(agg["total"])
        max_score = float(agg["max"])
        avg = (total / count) if count else 0.0
        identities.append(
            {
                "address": addr,
                "totalGradeScore": total,
                "flipCount": count,
                "avgGradeScore": avg,
                "maxFlipGradeScore": max_score,
            }
        )

    identities_sorted = sorted(
        identities,
        key=lambda x: (x["totalGradeScore"], x["flipCount"], x["address"]),
        reverse=True,
    )

    csv_path = os.path.join(out_dir, f"grade_identity_leaderboard_epoch{epoch}.csv")
    csv_rows: List[List[Any]] = []
    for i, row in enumerate(identities_sorted, start=1):
        csv_rows.append(
            [
                i,
                f"{row['totalGradeScore']:.8f}",
                row["flipCount"],
                f"{row['avgGradeScore']:.8f}",
                f"{row['maxFlipGradeScore']:.8f}",
                row["address"],
                f"https://scan.idena.io/address/{row['address']}",
            ]
        )

    write_csv(
        csv_path,
        header=[
            "rank",
            "totalGradeScore",
            "flipCount",
            "avgGradeScore",
            "maxFlipGradeScore",
            "address",
            "scan_url",
        ],
        rows=csv_rows,
    )

    top_rows = identities_sorted[: max(0, top_n)]
    top_identities: List[Dict[str, Any]] = []
    identity_lookup: List[Dict[str, Any]] = []
    rank_by_address: Dict[str, int] = {}
    for i, row in enumerate(identities_sorted, start=1):
        rank_by_address[row["address"]] = i
        identity_lookup.append(
            {
                "rank": i,
                "address": row["address"],
                "totalGradeScore": row["totalGradeScore"],
                "flipCount": row["flipCount"],
                "avgGradeScore": row["avgGradeScore"],
                "maxFlipGradeScore": row["maxFlipGradeScore"],
                "scanUrl": f"https://scan.idena.io/address/{row['address']}",
            }
        )

    for i, row in enumerate(top_rows, start=1):
        top_identities.append(
            {
                "rank": i,
                "address": row["address"],
                "totalGradeScore": row["totalGradeScore"],
                "flipCount": row["flipCount"],
                "avgGradeScore": row["avgGradeScore"],
                "maxFlipGradeScore": row["maxFlipGradeScore"],
                "scanUrl": f"https://scan.idena.io/address/{row['address']}",
            }
        )

    top_flips_sorted = sorted(
        [item[2] for item in flip_heap],
        key=lambda x: (x["gradeScore"], x["cid"]),
        reverse=True,
    )
    top_flips: List[Dict[str, Any]] = []
    for i, row in enumerate(top_flips_sorted, start=1):
        author_rank = rank_by_address.get(row["author"])
        top_flips.append(
            {
                "rank": i,
                "cid": row["cid"],
                "author": row["author"],
                "authorRank": author_rank,
                "gradeScore": row["gradeScore"],
                "status": row["status"],
                "word1": row["word1"],
                "word2": row["word2"],
                "scanUrl": f"https://scan.idena.io/flip/{row['cid']}",
                "authorScanUrl": f"https://scan.idena.io/address/{row['author']}",
            }
        )

    return {
      ...
    }
