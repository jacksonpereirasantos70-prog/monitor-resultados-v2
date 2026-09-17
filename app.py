import json
import logging
import os
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import median
from zoneinfo import ZoneInfo

from flask import Flask, jsonify
from google.cloud import firestore


app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("monitor-resultados-v2")

JONBET_URL = os.getenv(
    "JONBET_URL",
    "https://jonbet.bet.br/api/singleplayer-originals/"
    "originals/roulette_games/recent/1",
)
MIN_HISTORY = int(os.getenv("MIN_HISTORY", "1000"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "5000"))

ROUNDS = "v2_rounds"
PREDICTIONS = "v2_predictions"
STATE = "v2_system_state"

db = firestore.Client()
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def brazil_time(value):
    parsed = parse_time(value)
    if parsed is None:
        return "horário ainda não identificado"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(BRAZIL_TZ).strftime("%d/%m/%Y às %H:%M:%S")


def estimate_next_time(history):
    times = [parse_time(item.get("created_at")) for item in history[-30:]]
    times = [value for value in times if value is not None]
    intervals = []
    for previous, current in zip(times, times[1:]):
        seconds = (current - previous).total_seconds()
        if 1 <= seconds <= 600:
            intervals.append(seconds)
    interval = median(intervals[-15:]) if intervals else 60
    latest = times[-1] if times else datetime.now(timezone.utc)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    expected = latest + timedelta(seconds=interval)
    return expected.isoformat(), round(interval, 2)


def fetch_jonbet():
    request = urllib.request.Request(
        JONBET_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


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
    return {0: "branco", 1: "verde", 2: "escuro"}.get(color, str(color))


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


def pending_prediction():
    documents = (
        db.collection(PREDICTIONS)
        .where("resolved", "==", False)
        .limit(1)
        .get()
    )
    if not documents:
        return None, None
    data = documents[0].to_dict()
    data["prediction_id"] = documents[0].id
    return documents[0], data


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
        "created_at": utc_now(),
        "resolved": False,
    }


def resolve_pending_prediction(new_rounds):
    if not new_rounds:
        return None
    document, prediction = pending_prediction()
    if document is None:
        return None
    actual = sorted(new_rounds, key=lambda item: item.get("created_at") or "")[0]
    result = {
        "resolved": True,
        "actual_round_id": actual.get("id"),
        "actual_roll": actual.get("roll"),
        "actual_color": actual.get("color"),
        "actual_color_name": color_name(actual.get("color")),
        "roll_correct": prediction.get("predicted_roll") == actual.get("roll"),
        "color_correct": prediction.get("predicted_color") == actual.get("color"),
        "actual_created_at": actual.get("created_at"),
        "actual_created_at_display": brazil_time(actual.get("created_at")),
        "resolved_at": utc_now(),
    }
    document.reference.update(result)
    return result


def create_next_prediction(total=None):
    total = get_round_count() if total is None else total
    if total < MIN_HISTORY:
        return None
    _, current = pending_prediction()
    if current:
        return current
    prediction = make_prediction(get_history())
    if prediction is None:
        return None
    reference = db.collection(PREDICTIONS).document()
    prediction["prediction_id"] = reference.id
    prediction["history_size"] = total
    reference.set(prediction)
    return prediction


def save_state(**values):
    values["updated_at"] = utc_now()
    db.collection(STATE).document("main").set(values, merge=True)


def collect():
    try:
        source_status, raw_data = fetch_jonbet()
        incoming = sorted(
            extract_rounds(raw_data), key=lambda item: item.get("created_at") or ""
        )
        new_rounds = []
        for item in incoming:
            normalized = normalize_round(item)
            if normalized is None:
                continue
            reference = db.collection(ROUNDS).document(normalized["id"])
            if not reference.get().exists:
                reference.set(normalized)
                new_rounds.append(normalized)

        resolved = resolve_pending_prediction(new_rounds)
        total = get_round_count()
        prediction = create_next_prediction(total) if total >= MIN_HISTORY else None
        save_state(
            status="ok",
            last_error=None,
            last_collection=utc_now(),
            total_rounds=total,
            last_new_rounds=len(new_rounds),
            source_status=source_status,
        )
        return {
            "success": True,
            "source_status": source_status,
            "received": len(incoming),
            "new_rounds": len(new_rounds),
            "total_rounds": total,
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


def prediction_stats():
    resolved = [
        document.to_dict()
        for document in db.collection(PREDICTIONS).where("resolved", "==", True).get()
    ]
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


def recent_predictions(limit=10):
    documents = (
        db.collection(PREDICTIONS)
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .get()
    )
    return [document.to_dict() for document in documents]


def state_data():
    document = db.collection(STATE).document("main").get()
    return document.to_dict() if document.exists else {}


@app.route("/")
def home():
    try:
        total = get_round_count()
        stats = prediction_stats()
        recent = recent_predictions()
        _, current = pending_prediction()
        state = state_data()
        progress = min(100, round(total / MIN_HISTORY * 100, 1))
        if total < MIN_HISTORY:
            estimate = (
                f"<p>Coletando histórico. Faltam <strong>{MIN_HISTORY-total}</strong> "
                "resultados únicos para iniciar as estimativas.</p>"
            )
        elif current:
            estimate = f"""
            <p class="target">Previsão para <strong>{current.get('predicted_for_display', brazil_time(current.get('predicted_for')))}</strong></p>
            <div class="prediction-grid">
              <div><span>Próxima cor estimada</span><strong>{current.get('predicted_color_name')}</strong><small>{current.get('color_confidence')}% de frequência na amostra</small></div>
              <div><span>Próximo número estimado</span><strong>{current.get('predicted_roll')}</strong><small>{current.get('roll_confidence')}% de frequência na amostra</small></div>
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
</main></body></html>"""
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/health")
def health():
    return jsonify(status="online", service="monitor-resultados-v2", time=utc_now())


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
        return jsonify(collect())
    except Exception as error:
        return jsonify(success=False, error=str(error)), 500


@app.route("/api/stats")
def api_stats():
    try:
        total = get_round_count()
        _, current = pending_prediction()
        return jsonify(
            success=True,
            total_rounds=total,
            minimum_history=MIN_HISTORY,
            prediction_enabled=total >= MIN_HISTORY,
            current_prediction=current,
            performance=prediction_stats(),
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
