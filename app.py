import json
import logging
import os
import threading
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request
from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore


app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("monitor-resultados-v2")
APP_VERSION = "2026.09.24.5"

JONBET_URL = os.getenv(
    "JONBET_URL",
    "https://jonbet.bet.br/api/singleplayer-originals/"
    "originals/roulette_games/recent/1",
)
MIN_HISTORY = int(os.getenv("MIN_HISTORY", "1000"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "5000"))
PREDICTION_HISTORY_LIMIT = max(
    200, min(HISTORY_LIMIT, int(os.getenv("PREDICTION_HISTORY_LIMIT", "1000")))
)

ROUNDS = "v2_rounds"
PREDICTIONS = "v2_predictions"
STATE = "v2_system_state"

db = firestore.Client()
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
COLLECT_INTERVAL_SECONDS = max(3.0, float(os.getenv("COLLECT_INTERVAL_SECONDS", "5")))
DEFAULT_ROUND_INTERVAL_SECONDS = float(os.getenv("ROUND_INTERVAL_SECONDS", "30.081"))
MAX_BACKFILL_PAGES = max(1, min(50, int(os.getenv("MAX_BACKFILL_PAGES", "20"))))
STALE_AFTER_SECONDS = max(45.0, float(os.getenv("STALE_AFTER_SECONDS", "75")))
MAX_TARGET_LAG_SECONDS = max(5.0, float(os.getenv("MAX_TARGET_LAG_SECONDS", "15")))
MIN_PREDICTION_LEAD_SECONDS = max(
    1.0, float(os.getenv("MIN_PREDICTION_LEAD_SECONDS", "2"))
)
# O monitor normal já desperta o V2 a cada coleta. Manter outro ciclo interno
# no V2 criava duas coletas concorrentes e uma fila permanente de requisições.
BACKGROUND_COLLECTOR = os.getenv("V2_BACKGROUND_COLLECTOR", "false").lower() in ("1", "true", "yes", "on")
COLLECT_LOCK = threading.Lock()
COLLECTOR_STARTED = False
STATS_CACHE_LOCK = threading.Lock()
STATS_CACHE = None
STATS_CACHE_AT = 0.0
STATS_CACHE_SECONDS = max(60, int(os.getenv("STATS_CACHE_SECONDS", "300")))
STATS_REFRESH_LOCK = threading.Lock()
STATS_REFRESHING = False
STATE_CACHE_LOCK = threading.Lock()
STATE_CACHE = {}
STATE_CACHE_AT = 0.0
STATE_CACHE_SECONDS = max(5, int(os.getenv("STATE_CACHE_SECONDS", "30")))


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_time(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        return None


def prediction_timing_valid(item):
    created = parse_time(item.get("created_at"))
    actual = parse_time(item.get("actual_created_at"))
    target = parse_time(item.get("predicted_for"))
    if created is None or actual is None or actual <= created:
        return False
    if target is None:
        return True
    offset = (actual - target).total_seconds()
    return -5 <= offset <= MAX_TARGET_LAG_SECONDS


def deduplicate_resolved_predictions(items):
    """Keep one deterministic prediction for each real Jonbet round."""
    ordered = sorted(
        items,
        key=lambda item: (
            item.get("actual_created_at") or item.get("resolved_at") or "",
            item.get("created_at") or "",
            item.get("prediction_id") or "",
        ),
    )
    unique = []
    seen = set()
    for item in ordered:
        key = item.get("actual_round_id")
        if not key:
            key = ("prediction", item.get("prediction_id"), item.get("created_at"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def brazil_time(value):
    parsed = parse_time(value)
    if parsed is None:
        return "horário ainda não identificado"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(BRAZIL_TZ).strftime("%d/%m/%Y às %H:%M:%S")


def estimate_next_time(history):
    times = [parse_time(item.get("created_at")) for item in history[-60:]]
    times = sorted({value for value in times if value is not None})
    latest = times[-1] if times else datetime.now(timezone.utc)
    intervals = [
        (current - previous).total_seconds()
        for previous, current in zip(times, times[1:])
        if 20 <= (current - previous).total_seconds() <= 45
    ]
    interval = median(intervals[-40:]) if intervals else DEFAULT_ROUND_INTERVAL_SECONDS
    interval = round(min(35.0, max(25.0, interval)), 3)
    minimum_target = datetime.now(timezone.utc) + timedelta(
        seconds=MIN_PREDICTION_LEAD_SECONDS
    )
    rounds_ahead = 1
    expected = latest + timedelta(seconds=interval)
    while expected <= minimum_target:
        rounds_ahead += 1
        expected = latest + timedelta(seconds=interval * rounds_ahead)
    return expected.isoformat(), interval

def jonbet_page_url(page):
    base, separator, tail = JONBET_URL.rpartition("/")
    return f"{base}/{page}" if separator and tail.isdigit() else JONBET_URL


def fetch_jonbet_page(page=1):
    request = urllib.request.Request(
        jonbet_page_url(page),
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    last_error = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except Exception as error:
            last_error = error
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    raise last_error


def fetch_jonbet():
    return fetch_jonbet_page(1)


def fetch_jonbet_history(last_round_id=None):
    """Fetch pages until the last stored round is reached after an outage."""
    combined = []
    seen = set()
    source_status = None
    reached_last_known = last_round_id is None
    pages_fetched = 0
    for page in range(1, MAX_BACKFILL_PAGES + 1):
        source_status, payload = fetch_jonbet_page(page)
        pages_fetched += 1
        page_rounds = extract_rounds(payload)
        if not page_rounds:
            break
        for item in page_rounds:
            round_id = str(item.get("id")) if item.get("id") is not None else None
            if round_id == last_round_id:
                reached_last_known = True
            if round_id and round_id not in seen:
                seen.add(round_id)
                combined.append(item)
        if reached_last_known or last_round_id is None:
            break
    return source_status, combined, pages_fetched, reached_last_known


def extract_rounds(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("records", "results", "data", "rounds"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def normalize_round(item):
    round_id = item.get("id")
    if round_id is None:
        return None
    return {
        "id": str(round_id),
        "roll": item.get("roll"),
        "color": item.get("color"),
        "created_at": item.get("created_at"),
        "server_seed": item.get("server_seed"),
        "stored_at": utc_now(),
    }


def color_name(color):
    return {0: "branco", 1: "verde", 2: "preto"}.get(color, str(color))


def get_round_count():
    result = db.collection(ROUNDS).count().get()
    return result[0][0].value if result else 0


def get_history(limit=HISTORY_LIMIT):
    # DESC + limit evita o erro do Firestore causado por limit_to_last().stream().
    documents = (
        db.collection(ROUNDS)
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .get()
    )
    history = [document.to_dict() for document in documents]
    history.reverse()
    return history


def get_latest_round():
    history = get_history(limit=1)
    return history[-1] if history else None


def freshness_snapshot():
    # O documento de estado pode ser gravado fora de ordem quando o Cloud Run
    # usa mais de uma instância. A coleção de rodadas é a fonte confiável.
    latest = get_latest_round() or {}
    actual_time = parse_time(latest.get("created_at"))
    age = (
        round((datetime.now(timezone.utc) - actual_time).total_seconds(), 3)
        if actual_time is not None else None
    )
    fresh = age is not None and age <= STALE_AFTER_SECONDS
    return {
        "fresh": fresh,
        "status": "ok" if fresh else "stale",
        "last_round_id": latest.get("id") if latest else None,
        "last_round_created_at": latest.get("created_at") if latest else None,
        "seconds_since_last_round": age,
        "stale_after_seconds": STALE_AFTER_SECONDS,
        "collector_interval_seconds": COLLECT_INTERVAL_SECONDS,
    }


def pending_prediction():
    documents = (
        db.collection(PREDICTIONS)
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(50)
        .get()
    )
    documents = [item for item in documents if item.to_dict().get("resolved") is False]
    if not documents:
        return None, None
    document = max(documents, key=lambda item: item.to_dict().get("created_at") or "")
    data = document.to_dict()
    data["prediction_id"] = document.id
    return document, data


def make_prediction(history):
    if len(history) < MIN_HISTORY:
        return None

    rolls = [item.get("roll") for item in history]
    colors = [item.get("color") for item in history]
    roll_candidates, color_candidates = [], []
    roll_match_length, color_match_length = 0, 0

    # Cada mercado escolhe independentemente a melhor sequência com amostra >= 3.
    for length in (10, 9, 8, 7, 6, 5, 4, 3):
        if len(history) <= length:
            continue
        recent_rolls = rolls[-length:]
        candidates = [
            rolls[index + length]
            for index in range(len(history) - length)
            if rolls[index:index + length] == recent_rolls
            and rolls[index + length] is not None
        ]
        if len(candidates) >= 3:
            roll_candidates = candidates
            roll_match_length = length
            break

    for length in (10, 9, 8, 7, 6, 5, 4, 3):
        if len(history) <= length:
            continue
        recent_colors = colors[-length:]
        candidates = [
            colors[index + length]
            for index in range(len(history) - length)
            if colors[index:index + length] == recent_colors
            and colors[index + length] is not None
        ]
        if len(candidates) >= 3:
            color_candidates = candidates
            color_match_length = length
            break

    roll_method = "sequência"
    color_method = "sequência"
    if not roll_candidates:
        roll_candidates = [value for value in rolls if value is not None]
        roll_method = "frequência histórica"
    if not color_candidates:
        color_candidates = [value for value in colors if value is not None]
        color_method = "frequência histórica"
    if not roll_candidates or not color_candidates:
        return None

    roll_counter = Counter(roll_candidates)
    color_counter = Counter(color_candidates)
    predicted_roll, roll_hits = roll_counter.most_common(1)[0]
    predicted_color, color_hits = color_counter.most_common(1)[0]

    predicted_for, estimated_interval = estimate_next_time(history)
    latest_round = history[-1]
    latest_time = parse_time(latest_round.get("created_at"))
    predicted_time = parse_time(predicted_for)
    prediction_horizon_rounds = (
        max(1, round((predicted_time - latest_time).total_seconds() / estimated_interval))
        if latest_time is not None and predicted_time is not None and estimated_interval > 0
        else 1
    )

    return {
        "predicted_roll": predicted_roll,
        "predicted_color": predicted_color,
        "predicted_color_name": color_name(predicted_color),
        "roll_confidence": round(roll_hits / len(roll_candidates) * 100, 2),
        "color_confidence": round(color_hits / len(color_candidates) * 100, 2),
        "roll_match_length": roll_match_length,
        "color_match_length": color_match_length,
        "roll_sample_size": len(roll_candidates),
        "color_sample_size": len(color_candidates),
        "roll_method": roll_method,
        "color_method": color_method,
        "based_on_round_id": latest_round.get("id"),
        "based_on_round_created_at": latest_round.get("created_at"),
        "predicted_for": predicted_for,
        "predicted_for_display": brazil_time(predicted_for),
        "estimated_interval_seconds": estimated_interval,
        "prediction_horizon_rounds": prediction_horizon_rounds,
        "created_at": utc_now(),
        "resolved": False,
    }


def _resolve_prediction(document, prediction, available_rounds):
    if not available_rounds:
        return None

    prediction_created = parse_time(prediction.get("created_at"))
    predicted_for = parse_time(prediction.get("predicted_for"))
    based_on_time = parse_time(prediction.get("based_on_round_created_at"))
    based_on_round_id = str(prediction.get("based_on_round_id") or "")
    candidates = []
    for candidate in available_rounds:
        actual_time = parse_time(candidate.get("created_at"))
        if actual_time is None:
            continue
        candidate_id = str(candidate.get("id") or "")
        if based_on_round_id and candidate_id == based_on_round_id:
            continue
        # A previsão pertence à primeira rodada posterior à rodada-base. A
        # fonte pode divulgar o resultado depois do horário gravado na rodada.
        if based_on_time is not None and actual_time <= based_on_time:
            continue
        if predicted_for is not None:
            if actual_time < predicted_for - timedelta(seconds=5):
                continue
            if actual_time > predicted_for + timedelta(seconds=MAX_TARGET_LAG_SECONDS):
                continue
        if based_on_time is None and predicted_for is None:
            if prediction_created is not None and actual_time <= prediction_created:
                continue
        candidates.append((actual_time, candidate))

    if not candidates:
        return None

    actual_time, actual = min(
        candidates,
        key=lambda pair: (
            abs((pair[0] - predicted_for).total_seconds())
            if predicted_for is not None else pair[0].timestamp()
        ),
    )
    lead_time = (
        round((actual_time - prediction_created).total_seconds(), 3)
        if prediction_created is not None else None
    )
    target_offset = (
        round((actual_time - predicted_for).total_seconds(), 3)
        if predicted_for is not None else None
    )
    created_before_result = (
        prediction_created is not None and actual_time > prediction_created
    )
    target_in_window = (
        target_offset is None
        or -5 <= target_offset <= MAX_TARGET_LAG_SECONDS
    )
    timing_valid = created_before_result and target_in_window
    invalid_reason = None
    if not created_before_result:
        invalid_reason = "prediction_created_after_actual"
    elif not target_in_window:
        invalid_reason = "target_window_missed"
    result = {
        "resolved": True,
        "expired": not timing_valid,
        "invalid_reason": invalid_reason,
        "actual_round_id": actual.get("id"),
        "actual_roll": actual.get("roll"),
        "actual_color": actual.get("color"),
        "actual_color_name": color_name(actual.get("color")),
        "roll_correct": (
            prediction.get("predicted_roll") == actual.get("roll")
            if timing_valid else None
        ),
        "color_correct": (
            prediction.get("predicted_color") == actual.get("color")
            if timing_valid else None
        ),
        "actual_created_at": actual.get("created_at"),
        "actual_created_at_display": brazil_time(actual.get("created_at")),
        "lead_time_seconds": lead_time,
        "target_offset_seconds": target_offset,
        "timing_valid": timing_valid,
        "resolved_at": utc_now(),
    }
    document.reference.update(result)
    return result


def resolve_pending_prediction(available_rounds):
    if not available_rounds:
        return None
    documents = (
        db.collection(PREDICTIONS)
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(40)
        .get()
    )
    documents = [item for item in documents if item.to_dict().get("resolved") is False]
    resolved = []
    for document in sorted(
        documents,
        key=lambda item: item.to_dict().get("predicted_for") or "",
    ):
        prediction = document.to_dict()
        result = _resolve_prediction(document, prediction, available_rounds)
        if result:
            result["prediction_id"] = document.id
            resolved.append(result)
    return resolved[-1] if resolved else None

def create_next_prediction(total=None):
    total = get_round_count() if total is None else total
    if total < MIN_HISTORY:
        return None
    _, current = pending_prediction()
    if current:
        current_target = parse_time(current.get("predicted_for"))
        current_created = parse_time(current.get("created_at"))
        if (
            current_target is not None
            and current_created is not None
            and current_created < current_target
            and current_target
            > datetime.now(timezone.utc) + timedelta(seconds=MIN_PREDICTION_LEAD_SECONDS)
        ):
            return current
    prediction = make_prediction(get_history(limit=PREDICTION_HISTORY_LIMIT))
    if prediction is None:
        return None
    predicted_for = parse_time(prediction.get("predicted_for"))
    if (
        predicted_for is None
        or predicted_for
        <= datetime.now(timezone.utc) + timedelta(seconds=MIN_PREDICTION_LEAD_SECONDS)
    ):
        # Nunca crie uma previsão para uma rodada já ocorrida ou sem tempo útil.
        return None
    based_on_round_id = str(prediction.get("based_on_round_id") or "")
    if not based_on_round_id:
        return None
    # Todas as instâncias usam o mesmo documento para o mesmo ciclo de 30 s.
    # Isso bloqueia previsões duplicadas mesmo que os relógios estimados
    # difiram por alguns milissegundos ou a rodada-base ainda esteja atrasada.
    target_slot = int(round(predicted_for.timestamp() / 30.0))
    reference = db.collection(PREDICTIONS).document(f"target_slot_{target_slot}")
    existing = reference.get()
    if existing.exists:
        data = existing.to_dict()
        data["prediction_id"] = existing.id
        return None if data.get("resolved") else data
    prediction["prediction_id"] = reference.id
    prediction["history_size"] = total
    try:
        reference.create(prediction)
        return prediction
    except AlreadyExists:
        # Horário-alvo + rodada-base bloqueiam duplicatas entre instâncias sem
        # colidir com uma previsão antiga da mesma rodada-base.
        existing = reference.get()
        if not existing.exists:
            return None
        data = existing.to_dict()
        data["prediction_id"] = existing.id
        return None if data.get("resolved") else data


def save_state(**values):
    global STATE_CACHE, STATE_CACHE_AT
    values["updated_at"] = utc_now()
    db.collection(STATE).document("main").set(values, merge=True)
    with STATE_CACHE_LOCK:
        STATE_CACHE = {**STATE_CACHE, **values}
        STATE_CACHE_AT = time.monotonic()


def collect(blocking=True):
    acquired = COLLECT_LOCK.acquire(blocking=blocking)
    if not acquired:
        return {
            "success": True,
            "busy": True,
            "message": "Uma coleta já está em andamento.",
        }
    try:
        return collect_locked()
    finally:
        COLLECT_LOCK.release()


def collect_locked():
    try:
        latest = get_latest_round()
        last_round_id = str(latest.get("id")) if latest and latest.get("id") is not None else None
        source_status, raw_data, pages_fetched, reached_last_known = fetch_jonbet_history(last_round_id)
        incoming = sorted(
            raw_data, key=lambda item: item.get("created_at") or ""
        )
        new_rounds = []
        available_rounds = []
        references = []
        for item in incoming:
            normalized = normalize_round(item)
            if normalized is None:
                continue
            available_rounds.append(normalized)
            references.append(db.collection(ROUNDS).document(normalized["id"]))

        # Uma única leitura e uma única gravação em lote substituem dezenas de
        # viagens sequenciais ao Firestore a cada rodada.
        existing_ids = {
            document.id for document in db.get_all(references) if document.exists
        } if references else set()
        batch = db.batch()
        for normalized, reference in zip(available_rounds, references):
            if normalized["id"] not in existing_ids:
                batch.set(reference, normalized)
                new_rounds.append(normalized)
        if new_rounds:
            batch.commit()

        resolved = None
        prediction = None
        prediction_error = None
        try:
            # Normal e V2 podem salvar a mesma rodada antes um do outro. Mesmo
            # já armazenada, a rodada retornada pela fonte resolve a previsão.
            resolved = resolve_pending_prediction(available_rounds)
        except Exception as error:
            prediction_error = f"resolve: {error}"
            logger.exception("Resultados salvos, mas houve falha ao conferir a previsão")
        total = get_round_count()
        try:
            prediction = create_next_prediction(total) if total >= MIN_HISTORY else None
        except Exception as error:
            prediction_error = f"create: {error}"
            logger.exception("Resultados salvos, mas houve falha ao criar a próxima previsão")
        save_state(
            status="ok" if prediction_error is None else "degraded",
            last_error=prediction_error,
            last_collection=utc_now(),
            total_rounds=total,
            last_new_rounds=len(new_rounds),
            source_status=source_status,
            pages_fetched=pages_fetched,
            backfill_complete=reached_last_known,
            last_round_id=(available_rounds[-1].get("id") if available_rounds else last_round_id),
            last_round_created_at=(available_rounds[-1].get("created_at") if available_rounds else (latest or {}).get("created_at")),
            current_prediction=prediction,
        )
        if resolved:
            schedule_stats_refresh()
        return {
            "success": True,
            "source_status": source_status,
            "received": len(incoming),
            "new_rounds": len(new_rounds),
            "total_rounds": total,
            "pages_fetched": pages_fetched,
            "backfill_complete": reached_last_known,
            "prediction_error": prediction_error,
            "prediction_enabled": total >= MIN_HISTORY,
            "resolved_prediction": resolved,
            "prediction": prediction,
        }
    except Exception as error:
        logger.exception("Falha durante a coleta")
        try:
            save_state(status="error", last_error=str(error), last_failure=utc_now())
        except Exception:
            logger.exception("Falha ao salvar o estado de erro")
        raise


def collector_worker():
    logger.info("Coletor permanente iniciado; intervalo=%ss", COLLECT_INTERVAL_SECONDS)
    while True:
        started_at = time.monotonic()
        try:
            result = collect()
            logger.info(
                "Coleta automática concluída: novos=%s total=%s",
                result.get("new_rounds"),
                result.get("total_rounds"),
            )
        except Exception:
            logger.exception("Falha no ciclo do coletor permanente")
        elapsed = time.monotonic() - started_at
        time.sleep(max(0.5, COLLECT_INTERVAL_SECONDS - elapsed))


def start_background_collector():
    global COLLECTOR_STARTED
    if not BACKGROUND_COLLECTOR or COLLECTOR_STARTED:
        return
    COLLECTOR_STARTED = True
    threading.Thread(
        target=collector_worker,
        name="permanent-collector-v2",
        daemon=True,
    ).start()


def prediction_stats():
    resolved = [
        item for item in (
            document.to_dict()
            for document in db.collection(PREDICTIONS).where("resolved", "==", True).get()
        )
        if prediction_timing_valid(item)
    ]
    resolved = deduplicate_resolved_predictions(resolved)
    total = len(resolved)
    roll_hits = sum(item.get("roll_correct") is True for item in resolved)
    color_hits = sum(item.get("color_correct") is True for item in resolved)
    return {
        "predictions_resolved": total,
        "roll_hits": roll_hits,
        "roll_errors": total - roll_hits,
        "roll_accuracy": round(roll_hits / total * 100, 2) if total else 0,
        "color_hits": color_hits,
        "color_errors": total - color_hits,
        "color_accuracy": round(color_hits / total * 100, 2) if total else 0,
    }


def calibration_analysis():
    """Group every resolved prediction by the percentage shown to the user."""
    resolved = [
        item for item in (
            document.to_dict()
            for document in db.collection(PREDICTIONS).where("resolved", "==", True).get()
        )
        if prediction_timing_valid(item)
    ]
    resolved = deduplicate_resolved_predictions(resolved)

    def percentage(value):
        try:
            return round(float(value), 2)
        except (TypeError, ValueError):
            return None

    def normalized_color(value):
        name = str(value if value is not None else "desconhecido").lower()
        return "preto" if name in ("escuro", "preto") else name

    color_groups = {}
    number_groups = {}
    for item in resolved:
        color_confidence = percentage(item.get("color_confidence"))
        if color_confidence is not None:
            predicted = normalized_color(item.get("predicted_color_name"))
            key = (predicted, color_confidence)
            group = color_groups.setdefault(
                key,
                {"cor_prevista": predicted, "porcentagem": color_confidence,
                 "aparicoes": 0, "acertos": 0, "erros": 0,
                 "resultados_reais": Counter()},
            )
            group["aparicoes"] += 1
            hit = item.get("color_correct") is True
            group["acertos"] += int(hit)
            group["erros"] += int(not hit)
            group["resultados_reais"][normalized_color(item.get("actual_color_name"))] += 1

        roll_confidence = percentage(item.get("roll_confidence"))
        predicted_roll = item.get("predicted_roll")
        if roll_confidence is not None and predicted_roll is not None:
            key = (str(predicted_roll), roll_confidence)
            group = number_groups.setdefault(
                key,
                {"numero_previsto": predicted_roll, "porcentagem": roll_confidence,
                 "aparicoes": 0, "acertos": 0, "erros": 0,
                 "resultados_reais": Counter()},
            )
            group["aparicoes"] += 1
            hit = item.get("roll_correct") is True
            group["acertos"] += int(hit)
            group["erros"] += int(not hit)
            group["resultados_reais"][str(item.get("actual_roll"))] += 1

    def finish(groups):
        rows = []
        for group in groups.values():
            total = group["aparicoes"]
            group["taxa_real_de_acerto"] = round(group["acertos"] / total * 100, 2)
            group["resultados_reais"] = dict(
                sorted(group["resultados_reais"].items(), key=lambda pair: (-pair[1], pair[0]))
            )
            rows.append(group)
        return rows

    colors = finish(color_groups)
    numbers = finish(number_groups)
    colors.sort(key=lambda row: (row["cor_prevista"], row["porcentagem"]))
    numbers.sort(key=lambda row: (str(row["numero_previsto"]), row["porcentagem"]))
    return {
        "previsoes_conferidas": len(resolved),
        "cores": colors,
        "numeros": numbers,
        "descricao": "Cada linha reúne uma cor ou número previsto e a porcentagem exata exibida.",
    }


def prediction_history_export(include_invalid=False, limit=0):
    fields = (
        "prediction_id", "created_at", "predicted_for", "history_size",
        "prediction_horizon_rounds",
        "based_on_round_id", "based_on_round_created_at",
        "predicted_color", "predicted_color_name", "color_confidence", "color_method",
        "predicted_roll", "roll_confidence", "roll_method", "resolved",
        "expired", "invalid_reason",
        "actual_round_id", "actual_created_at", "actual_color", "actual_color_name",
        "actual_roll", "color_correct", "roll_correct", "timing_valid",
        "lead_time_seconds", "target_offset_seconds", "resolved_at",
    )
    rows = []
    if limit:
        # O painel e o aplicativo normalmente pedem somente as linhas recentes.
        # Limitar antes da leitura evita baixar milhares de documentos para
        # depois descartar quase todos eles.
        scan_limit = min(5000, max(limit + 10, limit * 3))
        documents = (
            db.collection(PREDICTIONS)
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(scan_limit)
            .get()
        )
    else:
        documents = db.collection(PREDICTIONS).where("resolved", "==", True).get()
    for document in documents:
        item = document.to_dict()
        if item.get("resolved") is not True:
            continue
        if not include_invalid and not prediction_timing_valid(item):
            continue
        row = {field: item.get(field) for field in fields}
        row["prediction_id"] = row.get("prediction_id") or document.id
        row["predicted_color_name"] = (
            "preto" if row.get("predicted_color_name") == "escuro"
            else row.get("predicted_color_name")
        )
        row["actual_color_name"] = (
            "preto" if row.get("actual_color_name") == "escuro"
            else row.get("actual_color_name")
        )
        rows.append(row)
    rows.sort(key=lambda row: row.get("actual_created_at") or row.get("resolved_at") or "")
    rows = deduplicate_resolved_predictions(rows)
    return rows[-limit:] if limit else rows


def empty_prediction_stats():
    return {
        "predictions_resolved": 0,
        "roll_hits": 0,
        "roll_errors": 0,
        "roll_accuracy": 0,
        "color_hits": 0,
        "color_errors": 0,
        "color_accuracy": 0,
    }


def _refresh_prediction_stats():
    global STATS_CACHE, STATS_CACHE_AT, STATS_REFRESHING
    try:
        stats = prediction_stats()
        with STATS_CACHE_LOCK:
            STATS_CACHE = stats
            STATS_CACHE_AT = time.monotonic()
        save_state(performance_cache=stats, performance_cache_at=utc_now())
    except Exception:
        logger.exception("Falha ao atualizar estatísticas em segundo plano")
    finally:
        with STATS_REFRESH_LOCK:
            STATS_REFRESHING = False


def schedule_stats_refresh():
    global STATS_REFRESHING
    with STATS_REFRESH_LOCK:
        if STATS_REFRESHING:
            return False
        STATS_REFRESHING = True
    threading.Thread(
        target=_refresh_prediction_stats,
        name="prediction-stats-refresh-v2",
        daemon=True,
    ).start()
    return True


def cached_prediction_stats(force=False):
    global STATS_CACHE, STATS_CACHE_AT
    now = time.monotonic()
    with STATS_CACHE_LOCK:
        cached = dict(STATS_CACHE) if STATS_CACHE is not None else None
        fresh = cached is not None and now - STATS_CACHE_AT < STATS_CACHE_SECONDS
    if fresh and not force:
        return cached
    if force:
        _refresh_prediction_stats()
        with STATS_CACHE_LOCK:
            return dict(STATS_CACHE or empty_prediction_stats())

    persisted = state_data().get("performance_cache")
    if cached is None and isinstance(persisted, dict):
        cached = {**empty_prediction_stats(), **persisted}
        with STATS_CACHE_LOCK:
            STATS_CACHE = cached
            STATS_CACHE_AT = now
    schedule_stats_refresh()
    return cached or empty_prediction_stats()


def recent_predictions(limit=10):
    documents = (
        db.collection(PREDICTIONS)
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .get()
    )
    rows = [document.to_dict() for document in documents]
    for row in rows:
        if row.get("predicted_color_name") == "escuro":
            row["predicted_color_name"] = "preto"
        if row.get("actual_color_name") == "escuro":
            row["actual_color_name"] = "preto"
    return rows


def state_data(force=False):
    global STATE_CACHE, STATE_CACHE_AT
    now = time.monotonic()
    with STATE_CACHE_LOCK:
        if not force and STATE_CACHE and now - STATE_CACHE_AT < STATE_CACHE_SECONDS:
            return dict(STATE_CACHE)
    document = db.collection(STATE).document("main").get()
    data = document.to_dict() if document.exists else {}
    with STATE_CACHE_LOCK:
        STATE_CACHE = data
        STATE_CACHE_AT = now
    return dict(data)


def timing_analysis(limit=HISTORY_LIMIT):
    history = get_history(limit)
    rows = []
    for item in history:
        created = parse_time(item.get("created_at"))
        roll = item.get("roll")
        if created is None or not isinstance(roll, int):
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        rows.append((created, roll, item.get("color")))
    rows.sort(key=lambda row: row[0])

    number_counts = Counter(row[1] for row in rows)
    second_counts = Counter(row[0].second for row in rows)
    parity_counts = Counter("par" if row[0].second % 2 == 0 else "ímpar" for row in rows)
    interval_counts = Counter()
    for previous, current in zip(rows, rows[1:]):
        seconds = round((current[0] - previous[0]).total_seconds())
        if 0 < seconds <= 600:
            interval_counts[seconds] += 1

    by_second = {}
    for second in sorted(second_counts):
        rolls = Counter(row[1] for row in rows if row[0].second == second)
        by_second[f"{second:02d}"] = {
            "total": second_counts[second],
            "top_numbers": [
                {"number": number, "count": count, "percentage": round(count / second_counts[second] * 100, 2)}
                for number, count in rolls.most_common(5)
            ],
        }

    total = len(rows)
    return {
        "analyzed_rounds": total,
        "first_created_at": rows[0][0].isoformat() if rows else None,
        "last_created_at": rows[-1][0].isoformat() if rows else None,
        "number_frequency": [
            {"number": number, "count": count, "percentage": round(count / total * 100, 2)}
            for number, count in number_counts.most_common()
        ] if total else [],
        "second_frequency": [
            {"second": f"{second:02d}", "count": count, "percentage": round(count / total * 100, 2)}
            for second, count in second_counts.most_common()
        ] if total else [],
        "second_parity": {
            key: {"count": count, "percentage": round(count / total * 100, 2)}
            for key, count in parity_counts.items()
        } if total else {},
        "interval_frequency_seconds": [
            {"seconds": seconds, "count": count}
            for seconds, count in interval_counts.most_common(10)
        ],
        "numbers_by_second": by_second,
        "note": "Horário e frequência descrevem o histórico; não garantem o próximo resultado.",
    }


@app.after_request
def allow_public_analysis_cors(response):
    """Allow the Double laboratory to read public, read-only analysis endpoints."""
    public_paths = (
        "/api/health",
        "/api/rounds",
        "/api/stats",
        "/api/analysis/predictions",
        "/api/analysis/calibration",
        "/api/analysis/timing",
        "/api/calculator",
    )
    if request.path in public_paths:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/")
def home():
    try:
        # O painel nunca deve aguardar uma análise histórica completa.
        executor = ThreadPoolExecutor(max_workers=4)
        jobs = {
            "stats": executor.submit(cached_prediction_stats),
            "recent": executor.submit(recent_predictions),
            "current": executor.submit(pending_prediction),
            "state": executor.submit(state_data),
        }
        completed, pending = wait(jobs.values(), timeout=4)
        for job in pending:
            job.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        state = jobs["state"].result() if jobs["state"] in completed else {}
        total = int(state.get("total_rounds", 0))
        stats = jobs["stats"].result() if jobs["stats"] in completed else empty_prediction_stats()
        recent = jobs["recent"].result() if jobs["recent"] in completed else []
        _, current = jobs["current"].result() if jobs["current"] in completed else (None, state.get("current_prediction"))
        if current and current.get("predicted_color_name") == "escuro":
            current = {**current, "predicted_color_name": "preto"}
        progress = min(100, round(total / MIN_HISTORY * 100, 1))
        if total < MIN_HISTORY:
            estimate = (
                f"<p>Coletando histórico. Faltam <strong>{MIN_HISTORY-total}</strong> "
                "resultados únicos para iniciar as estimativas.</p>"
            )
        elif current:
            estimate = f"""
            <p class="target">Previsão para <strong id="target-time">{current.get('predicted_for_display', brazil_time(current.get('predicted_for')))}</strong></p>
            <div class="prediction-grid">
              <div><span>Próxima cor estimada</span><strong id="predicted-color">{current.get('predicted_color_name')}</strong><small id="color-confidence">{current.get('color_confidence')}% de frequência na amostra</small></div>
              <div><span>Próximo número estimado</span><strong id="predicted-roll">{current.get('predicted_roll')}</strong><small id="roll-confidence">{current.get('roll_confidence')}% de frequência na amostra</small></div>
            </div>
            <p class="muted">Métodos: cor — {current.get('color_method')}; número — {current.get('roll_method')}.</p>
            """
        else:
            estimate = "<p>Nenhuma estimativa pendente. Execute uma coleta para tentar criá-la.</p>"

        history_cards = []
        for item in recent:
            if not item.get("resolved"):
                status = '<span class="badge waiting">Aguardando resultado</span>'
                actual_text = "Resultado real ainda não chegou."
            else:
                color_status = "ACERTOU" if item.get("color_correct") else "ERROU"
                roll_status = "ACERTOU" if item.get("roll_correct") else "ERROU"
                status = '<span class="badge done">Conferida</span>'
                actual_text = (
                    f"Real: {item.get('actual_color_name')} — número {item.get('actual_roll')} "
                    f"({item.get('actual_created_at_display', brazil_time(item.get('actual_created_at')))})<br>"
                    f"Cor: <strong>{color_status}</strong> · Número: <strong>{roll_status}</strong>"
                )
            target = item.get("predicted_for_display", brazil_time(item.get("predicted_for")))
            history_cards.append(
                f'<div class="history-item">{status}<strong>{target}</strong><br>'
                f'Previsão: {item.get("predicted_color_name")} — número {item.get("predicted_roll")}<br>'
                f'<span>{actual_text}</span></div>'
            )
        history_html = "".join(history_cards) or "<p>Nenhuma previsão criada ainda.</p>"

        return f"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monitor de Resultados V2</title><style>
:root{{--bg:#f3f5f8;--card:#fff;--ink:#202124;--muted:#68707c;--accent:#5b34da;--ok:#18864b}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--ink);font:17px system-ui,-apple-system,sans-serif}}
main{{max-width:760px;margin:auto;padding:26px 18px 50px}} h1{{font-size:2.2rem;margin:.4rem 0}} h2{{font-size:1.55rem;margin-top:0}}
.subtitle,.muted{{color:var(--muted)}} .card{{background:var(--card);padding:27px;margin:22px 0;border-radius:22px;box-shadow:0 3px 14px #00000012}}
.value{{font-size:1.3rem;font-weight:750}} .bar{{height:22px;background:#dedfe4;border-radius:20px;overflow:hidden}} .bar i{{display:block;height:100%;width:{progress}%;background:linear-gradient(90deg,var(--accent),#8a67f1)}}
.prediction-grid,.stats{{display:grid;grid-template-columns:1fr 1fr;gap:14px}} .prediction-grid div,.stats div{{background:#f5f2ff;padding:17px;border-radius:15px}}
.prediction-grid span,.prediction-grid small{{display:block}} .prediction-grid strong{{display:block;font-size:1.55rem;margin:6px 0;text-transform:capitalize}}
.stats strong{{font-size:1.5rem}} a{{display:block;color:var(--accent);font-weight:650;margin:14px 0}} .ok{{color:var(--ok)}}
.target{{font-size:1.12rem;background:#fff4cf;padding:14px;border-radius:14px}} .history-item{{border-top:1px solid #e3e5e8;padding:16px 0;line-height:1.55}} .history-item:first-child{{border-top:0}}
.badge{{display:inline-block;font-size:.78rem;padding:4px 9px;border-radius:20px;margin-right:8px}} .waiting{{background:#fff0bd;color:#775600}} .done{{background:#dff5e8;color:#12633a}}
@media(max-width:520px){{h1{{font-size:1.8rem}}.card{{padding:22px}}.prediction-grid,.stats{{grid-template-columns:1fr}}}}
</style></head><body><main>
<h1>Monitor de Resultados V2</h1><p class="subtitle">Coleta, estimativas e conferência automática.</p>
<section class="card"><h2>Banco</h2><p>Resultados armazenados: <span class="value">{total}</span></p><p>Progresso: <strong>{total} / {MIN_HISTORY} — {progress}%</strong></p><div class="bar"><i></i></div></section>
<section class="card"><h2>Coleta automática</h2><p class="ok">Sistema online</p><p>Última coleta: <strong>{state.get('last_collection','ainda não executada')}</strong></p><p>Novos resultados: <strong>{state.get('last_new_rounds',0)}</strong></p></section>
<section class="card"><h2>Estimativa</h2>{estimate}<p class="muted">Estimativas estatísticas não garantem resultados futuros.</p></section>
<section class="card"><h2>Desempenho real</h2><p>Previsões conferidas: <strong>{stats['predictions_resolved']}</strong></p><div class="stats"><div>Cor<br><strong>{stats['color_accuracy']}%</strong><br>{stats['color_hits']} acertos / {stats['color_errors']} erros</div><div>Número<br><strong>{stats['roll_accuracy']}%</strong><br>{stats['roll_hits']} acertos / {stats['roll_errors']} erros</div></div></section>
<section class="card"><h2>Histórico das previsões</h2>{history_html}</section>
<section class="card"><h2>Ferramentas</h2><a href="/api/collect">Executar coleta agora</a><a href="/api/stats">Estatísticas em JSON</a><a href="/api/state">Estado do coletor</a><a href="/api/verify">Verificar sistema</a><a href="/api/rounds">Resultados atuais da fonte</a></section>
<script>
let syncing = false;
async function syncRound() {{
  if (syncing) return;
  syncing = true;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 4500);
  try {{
    const response = await fetch('/api/stats', {{cache:'no-store', signal:controller.signal}});
    if (!response.ok) throw new Error('HTTP ' + response.status);
    const prediction = (await response.json()).current_prediction;
    if (!prediction) return;
    const color = document.getElementById('predicted-color');
    if (!color) {{ location.reload(); return; }}
    document.getElementById('target-time').textContent = prediction.predicted_for_display || '';
    color.textContent = prediction.predicted_color_name ?? '-';
    document.getElementById('predicted-roll').textContent = prediction.predicted_roll ?? '-';
    document.getElementById('color-confidence').textContent = (prediction.color_confidence ?? 0) + '% de frequência na amostra';
    document.getElementById('roll-confidence').textContent = (prediction.roll_confidence ?? 0) + '% de frequência na amostra';
  }} catch (error) {{
    console.debug('Sincronização temporariamente indisponível', error);
  }} finally {{
    clearTimeout(timeout);
    syncing = false;
  }}
}}
syncRound();
setInterval(syncRound, 8000);
document.addEventListener('visibilitychange', () => {{
  if (document.visibilityState === 'visible') syncRound();
}});
window.addEventListener('online', syncRound);
</script>
</main></body></html>"""
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/health")
def health():
    try:
        freshness = freshness_snapshot()
        return jsonify(
            status="online" if freshness["fresh"] else "degraded",
            service="monitor-resultados-v2",
            version=APP_VERSION,
            time=utc_now(),
            freshness=freshness,
        )
    except Exception as error:
        return jsonify(status="degraded", service="monitor-resultados-v2", time=utc_now(), error=str(error)), 503


@app.route("/api/rounds")
def rounds():
    try:
        status_code, data = fetch_jonbet()
        return jsonify(success=True, source_status=status_code, rounds=data)
    except Exception as error:
        return jsonify(success=False, error=str(error)), 502


@app.route("/api/collect")
def api_collect():
    try:
        return jsonify(collect(blocking=False))
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/stats")
def api_stats():
    try:
        state = state_data()
        total = int(state.get("total_rounds", 0))
        _, current = pending_prediction()
        if current and current.get("predicted_color_name") == "escuro":
            current = {**current, "predicted_color_name": "preto"}
        return jsonify(
            success=True,
            total_rounds=total,
            minimum_history=MIN_HISTORY,
            prediction_enabled=total >= MIN_HISTORY,
            current_prediction=current,
            performance=cached_prediction_stats(),
        )
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/state")
def api_state():
    try:
        return jsonify(success=True, state=state_data(), total_rounds=get_round_count())
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/verify")
def api_verify():
    try:
        total = get_round_count()
        _, current = pending_prediction()
        return jsonify(
            success=True,
            checks={
                "firestore": "ok",
                "minimum_reached": total >= MIN_HISTORY,
                "pending_prediction": current is not None,
                "last_error": state_data().get("last_error"),
            },
            total_rounds=total,
            current_prediction=current,
        )
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/analysis/timing")
def api_timing_analysis():
    try:
        return jsonify(success=True, service="monitor-resultados-v2", analysis=timing_analysis())
    except Exception as error:
        logger.exception("Falha na análise temporal")
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/analysis/calibration")
def api_calibration_analysis():
    try:
        return jsonify(success=True, service="monitor-resultados-v2", analysis=calibration_analysis())
    except Exception as error:
        logger.exception("Falha na análise de calibração")
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/analysis/predictions")
def api_prediction_history():
    try:
        include_invalid = request.args.get("include_invalid", "").lower() in (
            "1", "true", "yes", "on"
        )
        try:
            limit = max(0, min(5000, int(request.args.get("limit", "0"))))
        except ValueError:
            limit = 0
        rows = prediction_history_export(
            include_invalid=include_invalid,
            limit=limit,
        )
        return jsonify(success=True, service="monitor-resultados-v2", total=len(rows), predictions=rows)
    except Exception as error:
        logger.exception("Falha na exportação do histórico de previsões")
        return jsonify(success=False, error=str(error)), 500


start_background_collector()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
