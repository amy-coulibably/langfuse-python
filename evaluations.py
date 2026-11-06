"""Les évaluations du correcteur déposées sur la trace de la session, dans la plateforme d'observabilité.

Leçon du premier POC (et de la recherche sur Arize AX, novembre 2026) : une évaluation écrite comme **annotation**
(`annotation.<nom>`) n'est pas vue par les widgets d'évaluation, l'onglet « Evals & Metrics » ni les moniteurs sur
`eval.<nom>.label`. Ici, le correcteur écrit donc des colonnes **`eval.<nom>.label | score | explanation`** sur le span racine
de la trace (`context.span_id`), par `client.spans.update_evaluations` du SDK Arize ; les annotations `annotation.*` restent
réservées aux avis humains (lot 6, file de relecture). Sur Phoenix (banc d'essai local), les mêmes évaluations partent comme
annotations de span de type CODE : Phoenix ne fait pas la distinction.

Les noms sont stables, pour que les moniteurs du lot 6 s'y réfèrent :

| nom | label | score |
| --- | --- | --- |
| `eval.chiffres_exacts` | EXACT / ECARTS / AUCUN | part des mesures chiffrées exactes |
| `eval.taux_effort` | EXACT / ECART / NON_CALCULE… | 1 ou 0 |
| `eval.recommandation` | position obtenue | 1 si celle attendue |
| `eval.doutes_attendus` | n/total | part des doutes attendus retrouvés |
| `eval.faux_doutes` | OUI / NON | nombre de doutes non attendus |
| `eval.moments_reflexivite` | n/total | part des quatre moments reconnus |
| `eval.synthese_verifiee` | OUI / NON | 1 ou 0 |
| `eval.reussite` | OUI / NON | 1 ou 0 : le critère de réussite des campagnes |

Tout est fail-open : plateforme injoignable ou span pas encore ingéré = avertissement, jamais une exception. Le SDK Arize
n'est pas installé dans l'environnement du POC (incompatible avec Phoenix sur `wrapt`) : la voie Arize est écrite et testée
avec un client simulé ; elle s'exerce sur le poste qui parle à l'instance.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Protocol

from poc.observabilite.config import ConfigObservabilite

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvaluationTrace:
    nom: str  # sans le préfixe eval.
    label: str
    score: float | None = None
    explication: str | None = None


class Puits(Protocol):
    def deposer(self, span_id: str, evaluations: list[EvaluationTrace]) -> int: ...


def nom_sur(nom: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", nom)


class PuitsInactif:
    def deposer(self, span_id: str, evaluations: list[EvaluationTrace]) -> int:
        return 0


class PuitsPhoenix:
    """Phoenix : annotations de span de type CODE, une par évaluation, nommées `eval.<nom>`."""

    def __init__(self, base_url: str, projet: str) -> None:
        from phoenix.client import Client

        self.client = Client(base_url=base_url)
        self.projet = projet

    def deposer(self, span_id: str, evaluations: list[EvaluationTrace]) -> int:
        n = 0
        for e in evaluations:
            try:
                self.client.spans.add_span_annotation(
                    span_id=span_id,
                    annotation_name=f"eval.{nom_sur(e.nom)}",
                    annotator_kind="CODE",
                    label=e.label,
                    score=e.score,
                    explanation=(e.explication or "")[:2000] or None,
                    sync=True,
                )
                n += 1
            except Exception:  # noqa: BLE001
                logger.warning("évaluation %s non envoyée à Phoenix", e.nom, exc_info=True)
        return n


class PuitsArize:
    """Arize AX : `spans.update_evaluations` sur une table `context.span_id` + colonnes `eval.<nom>.label|score|explanation`."""

    PAUSES_S = (5, 10, 15, 20, 30, 30, 30)

    def __init__(self, config: ConfigObservabilite, client: Any = None) -> None:
        if client is None:
            from arize import ArizeClient  # type: ignore[import-not-found]

            client = ArizeClient(**options_client_arize(config))
        self.client = client
        self.space_id = config.space_id
        self.projet = config.projet
        self.attente_s = config.attente_ingestion_s
        self.dormir = time.sleep
        options = options_client_arize(config)
        self.cible_flight = f"{options.get('single_host', '?')}:{options.get('single_port', 443)}"
        self.derniere_erreur: str | None = None

    @staticmethod
    def ligne(span_id: str, evaluations: list[EvaluationTrace]) -> dict[str, Any]:
        ligne: dict[str, Any] = {"context.span_id": span_id}
        for e in evaluations:
            nom = nom_sur(e.nom)
            ligne[f"eval.{nom}.label"] = str(e.label)
            if e.score is not None:
                ligne[f"eval.{nom}.score"] = float(e.score)
            if e.explication:
                ligne[f"eval.{nom}.explanation"] = e.explication[:4000]
        return ligne

    def deposer(self, span_id: str, evaluations: list[EvaluationTrace]) -> int:
        import pandas as pd

        if not evaluations:
            return 0
        _vider_traces_en_attente()
        df = pd.DataFrame([self.ligne(span_id, evaluations)])
        debut = time.monotonic()
        pauses = iter(self.PAUSES_S)
        attendu = 0.0
        journal_sdk = logging.getLogger("arize")
        niveau = journal_sdk.level
        while True:
            journal_sdk.setLevel(
                logging.CRITICAL
            )  # le SDK journalise lui-même chaque échec, pile comprise : on parle à sa place
            try:
                reponse = self.client.spans.update_evaluations(
                    dataframe=df, project_name=self.projet, space_id=self.space_id
                )
            except Exception as exc:  # noqa: BLE001
                self.derniere_erreur = resumer_erreur_sdk(exc, self.cible_flight, self.projet)
                logger.debug("détail de l'échec du SDK Arize", exc_info=True)
                if not projet_pas_encore_cree(exc):
                    print(f"[observabilité] évaluations non envoyées à Arize : {self.derniere_erreur}", file=sys.stderr)
                    return 0
                # Mac de la banque, 6 novembre 2026 : juste après la première trace d'un projet neuf, Flight répond
                # « project not found » ; le projet naît à l'ingestion de cette trace. Même traitement qu'un span absent.
                reponse = {"unmatched_ids": [span_id]}
            finally:
                journal_sdk.setLevel(niveau)
            verdict = verdict_envoi(reponse, span_id)
            if verdict == "ACCEPTE":
                return len(evaluations)
            if verdict != "SPAN_ABSENT":
                self.derniere_erreur = f"refus d'Arize : {str(reponse)[:300]}"
                logger.warning("évaluations refusées par Arize pour le span %s : %s", span_id, reponse)
                return 0
            ecoule = max(time.monotonic() - debut, attendu)
            pause = next(pauses, self.PAUSES_S[-1])
            if ecoule + pause > self.attente_s:
                if not (self.derniere_erreur or "").startswith("projet"):
                    self.derniere_erreur = f"span {span_id} toujours absent après {ecoule:.0f} s (ingestion lente)"
                print(
                    f"[observabilité] span {span_id} toujours absent d'Arize après {ecoule:.0f} s : évaluations non déposées. "
                    "Relancer plus tard `poc agent evaluer <session>`.",
                    file=sys.stderr,
                )
                return 0
            quoi = (
                f"projet « {self.projet} »" if (self.derniere_erreur or "").startswith("projet") else f"span {span_id}"
            )
            print(
                f"[observabilité] {quoi} pas encore ingéré par Arize, nouvel essai dans {pause} s…",
                file=sys.stderr,
            )
            attendu += pause
            self.dormir(pause)


def projet_pas_encore_cree(exc: Exception) -> bool:
    """Flight répond NOT_FOUND « project not found » tant que la première trace du projet n'est pas ingérée."""
    texte = str(exc).lower()
    return "project not found" in texte or ("not_found" in texte and "project" in texte)


def resumer_erreur_sdk(exc: Exception, cible: str, projet: str = "") -> str:
    """Une phrase au lieu d'une pile de quarante lignes, avec le réglage à vérifier."""
    texte = str(exc)
    if projet_pas_encore_cree(exc):
        return (
            f"projet « {projet} » pas encore connu d'Arize : il se crée à l'ingestion de sa première trace (quelques minutes). "
            "Si cela dure, vérifier que OBS_PROJECT et OBS_SPACE_ID sont ceux du projet visible dans Arize"
        )
    if "UNAVAILABLE" in texte or "failed to connect" in texte or "Connection refused" in texte:
        return (
            f"Flight injoignable sur {cible} : l'hôte ou le port du dépôt des évaluations diffère de celui des traces ? "
            "Régler OBS_ARIZE_HOTE et OBS_ARIZE_PORT dans .env (valeurs données par l'équipe plateforme)"
        )
    if "CERTIFICATE_VERIFY_FAILED" in texte or "SSL" in texte or "handshake" in texte.lower():
        return f"certificat refusé par Flight sur {cible} : vérifier POC_CA_BUNDLE (autorité interne, fichier PEM)"
    if "401" in texte or "403" in texte or "Unauthenticated" in texte or "PermissionDenied" in texte:
        return "clé ou espace refusés : vérifier OBS_API_KEY et OBS_SPACE_ID"
    return f"{type(exc).__name__} : {texte.splitlines()[0][:300] if texte else ''}"


def options_client_arize(config: ConfigObservabilite) -> dict[str, Any]:
    """Les paramètres du client Arize (SDK v8, vérifiés sur arize 8.64) pour une instance auto-hébergée : un seul hôte pour
    l'API, l'OTLP et Flight (celui de OBS_ENDPOINT, ou OBS_ARIZE_HOTE / OBS_ARIZE_PORT), et l'autorité interne
    (`ssl_ca_cert`) pour l'envoi Flight des évaluations."""
    from urllib.parse import urlparse

    from poc.observabilite.config import autorite_interne

    options: dict[str, Any] = {"api_key": config.api_key}
    u = urlparse(config.endpoint or "")
    hote = config.arize_hote or u.hostname
    port = config.arize_port or u.port
    if hote:
        options["single_host"] = hote
    if port:
        options["single_port"] = int(port)
    if u.scheme in ("http", "https"):
        options["api_scheme"] = u.scheme
    ca, _ = autorite_interne()
    if ca is not None and ca.is_file():
        options["ssl_ca_cert"] = str(ca)
    return options


def verdict_envoi(reponse: Any, span_id: str) -> str:
    """ACCEPTE, SPAN_ABSENT (pas encore ingéré) ou REFUSE, d'après la réponse du SDK (deux formes connues)."""
    if not isinstance(reponse, dict):
        return "ACCEPTE"
    absents = {str(i) for i in reponse.get("unmatched_ids") or []}
    autres = False
    for err in reponse.get("errors") or []:
        if "no matching span" in str(err.get("error_message", "")).lower():
            absents.add(str(err.get("span_id") or span_id))
        else:
            autres = True
    if autres:
        return "REFUSE"
    if absents:
        return "SPAN_ABSENT"
    maj = reponse.get("records_updated", reponse.get("spans_updated"))
    if maj is not None and int(maj) == 0 and int(reponse.get("spans_failed") or 0) > 0:
        return "REFUSE"
    return "ACCEPTE"


def _vider_traces_en_attente() -> None:
    try:
        from poc.observabilite.adaptateur import obtenir

        manager = obtenir().manager
        if manager is not None:
            manager.flush()
    except Exception:  # noqa: BLE001
        logger.debug("vidage des traces impossible avant dépôt", exc_info=True)


def base_url_depuis_endpoint(endpoint: str | None) -> str:
    if not endpoint:
        return "http://localhost:6006"
    for suffixe in ("/v1/traces", "/v1/traces/"):
        if endpoint.endswith(suffixe):
            return endpoint[: -len(suffixe)]
    return endpoint.rstrip("/")


def puits_pour(config: ConfigObservabilite) -> Puits:
    if not config.effective:
        return PuitsInactif()
    try:
        if config.backend == "phoenix":
            return PuitsPhoenix(base_url_depuis_endpoint(config.endpoint), config.projet)
        if config.backend == "arize":
            return PuitsArize(config)
    except Exception:  # noqa: BLE001
        logger.warning("puits d'évaluations indisponible (%s)", config.backend, exc_info=True)
    return PuitsInactif()


# ---------------------------------------------------------------------------------------------------------------------
# Du correcteur aux évaluations
# ---------------------------------------------------------------------------------------------------------------------
def evaluations_de(ev: Any) -> list[EvaluationTrace]:
    """Traduit une `Evaluation` du correcteur en évaluations de trace."""
    from poc.evaluation.campagne import reussite

    statuts = {m.nom: m for m in ev.mesures}
    chiffres = [
        m
        for m in ev.mesures
        if m.nom
        in (
            "taux_effort",
            "mensualite_25",
            "mensualite_20",
            "mensualite_15",
            "revenus_retenus",
            "charges_retenues",
            "reste_a_vivre",
            "montant_emprunte",
        )
        and m.statut != "NON_EVALUABLE"
    ]
    exacts = sum(1 for m in chiffres if m.statut == "EXACT")
    ecarts = [f"{m.nom} agent={m.obtenu} attendu={m.attendu}" for m in chiffres if m.statut == "ECART"]
    absents = [m.nom for m in chiffres if m.statut == "NON_CALCULE"]
    out = [
        EvaluationTrace(
            "chiffres_exacts",
            "EXACT" if chiffres and exacts == len(chiffres) else ("ECARTS" if exacts else "AUCUN"),
            round(exacts / len(chiffres), 3) if chiffres else 0.0,
            f"{exacts}/{len(chiffres)} chiffres exacts. "
            + (f"Écarts : {'; '.join(ecarts[:5])}. " if ecarts else "")
            + (f"Non calculés : {', '.join(absents)}." if absents else ""),
        )
    ]
    for nom in ("taux_effort", "recommandation"):
        m = statuts.get(nom)
        if m is not None:
            label = str(m.obtenu) if nom == "recommandation" and m.obtenu else m.statut
            out.append(
                EvaluationTrace(
                    nom,
                    label,
                    1.0 if m.statut == "EXACT" else 0.0,
                    f"attendu {m.attendu} ; obtenu {m.obtenu} ({m.statut}) {m.detail}".strip(),
                )
            )
    total = ev.doutes_attendus_total
    out.append(
        EvaluationTrace(
            "doutes_attendus",
            f"{ev.doutes_attendus_trouves}/{total}",
            round(ev.doutes_attendus_trouves / total, 3) if total else 1.0,
        )
    )
    out.append(
        EvaluationTrace(
            "faux_doutes",
            "OUI" if ev.faux_doutes else "NON",
            float(ev.faux_doutes),
            "doutes posés qui ne correspondent à aucun doute attendu",
        )
    )
    if ev.moments:
        ok = sum(1 for v in ev.moments.values() if v)
        out.append(
            EvaluationTrace(
                "moments_reflexivite",
                f"{ok}/{len(ev.moments)}",
                round(ok / len(ev.moments), 3),
                ", ".join(f"{k}={'oui' if v else 'non'}" for k, v in ev.moments.items()),
            )
        )
    m = statuts.get("synthese_produite")
    if m is not None:
        out.append(
            EvaluationTrace(
                "synthese_verifiee",
                "OUI" if m.statut == "EXACT" else "NON",
                1.0 if m.statut == "EXACT" else 0.0,
                m.detail,
            )
        )
    # ---- colonnes ajoutées pour les moniteurs (lecture directe d'un label OUI/NON ou d'un écart chiffré)
    m = statuts.get("recommandation")
    if m is not None:
        concordante = m.statut == "EXACT"
        out.append(
            EvaluationTrace(
                "recommandation_concordante",
                "OUI" if concordante else "NON",
                1.0 if concordante else 0.0,
                f"attendu {m.attendu} ; obtenu {m.obtenu or 'aucune recommandation'}",
            )
        )
    m = statuts.get("taux_effort")
    if m is not None:
        ecart = None
        if m.statut in ("EXACT", "ECART") and m.obtenu is not None and m.attendu is not None:
            ecart = round(abs(float(m.obtenu) - float(m.attendu)), 2)
        out.append(
            EvaluationTrace(
                "ecart_taux_effort",
                m.statut,
                ecart,
                f"écart en points de pourcentage ; attendu {m.attendu} %, obtenu {m.obtenu}",
            )
        )
    m = statuts.get("verdicts")
    if m is not None and isinstance(m.attendu, int) and m.attendu:
        justes = int(m.obtenu or 0)
        out.append(EvaluationTrace("conformite", f"{justes}/{m.attendu}", round(justes / m.attendu, 3), m.detail))
    m = statuts.get("verdict_hcsf")
    if m is not None:
        out.append(
            EvaluationTrace(
                "verdict_hcsf",
                m.statut,
                None if m.statut == "NON_EVALUABLE" else (1.0 if m.statut == "EXACT" else 0.0),
                f"attendu {m.attendu} ; obtenu {m.obtenu}",
            )
        )
    for nom, mesure in (
        ("faits_extraits", "faits_extraits"),
        ("pieces_demandees", "pieces_manquantes_demandees"),
        ("ocr_pieces_numerisees", "ocr_pieces_numerisees"),
    ):
        m = statuts.get(mesure)
        if m is not None and isinstance(m.attendu, int) and m.attendu:
            n = int(m.obtenu or 0)
            out.append(EvaluationTrace(nom, f"{n}/{m.attendu}", round(n / m.attendu, 3), m.detail or None))
    m = statuts.get("score_consulte")
    if m is not None:
        out.append(
            EvaluationTrace(
                "score_consulte",
                "OUI" if m.obtenu is not None else "NON",
                1.0 if m.statut == "EXACT" else 0.0,
                m.detail,
            )
        )
    ok = reussite(ev)
    out.append(
        EvaluationTrace(
            "reussite",
            "OUI" if ok else "NON",
            1.0 if ok else 0.0,
            "recommandation exacte, taux d'effort exact, verdict HCSF exact, synthèse vérifiée, aucun faux doute",
        )
    )
    return out


def publier_evaluation(config: ConfigObservabilite, ev: Any, trace: dict | None) -> dict[str, Any]:
    """Dépose les évaluations du correcteur sur le span racine de la session. Renvoie le nombre d'évaluations acceptées
    (`span`) et, en cas d'échec, la cause en une phrase (`erreur`)."""
    puits = puits_pour(config)
    if isinstance(puits, PuitsInactif) or not trace or not trace.get("span_id"):
        return {"span": 0}
    n = puits.deposer(trace["span_id"], evaluations_de(ev))
    return {"span": n, "erreur": getattr(puits, "derniere_erreur", None)}
