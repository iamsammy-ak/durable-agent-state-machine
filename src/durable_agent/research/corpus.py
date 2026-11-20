"""A small synthetic corpus for the offline search tool.

These notes were written for this repository's demo. They are not real publications, the URLs
are placeholders on example.org, and the statements are deliberately general.
"""

from __future__ import annotations

import re

from pydantic import BaseModel


class Document(BaseModel):
    doc_id: str
    topic: str
    title: str
    url: str
    text: str


_NOTES: tuple[tuple[str, str, str], ...] = (
    # (topic, title, text)
    ("heat pumps in cold climates", "How air-source heat pumps work below freezing",
     "An air-source heat pump moves heat from outdoor air into a building. Cold-climate models use variable-speed "
     "compressors and vapour injection so they keep delivering heat well below freezing, although efficiency "
     "falls as the outdoor temperature drops."),
    ("heat pumps in cold climates", "Field monitoring of heat pumps in cold regions",
     "Monitoring studies of homes in cold regions generally find seasonal efficiency well above that of electric "
     "resistance heating. Results vary with installation quality, duct design and how the system is sized."),
    ("heat pumps in cold climates", "Sizing and backup heat for cold-climate heat pumps",
     "Installers size cold-climate heat pumps against the design temperature of the site. Many systems keep a "
     "small backup heater for the coldest hours, which protects comfort but raises running costs if overused."),
    ("heat pumps in cold climates", "Defrost cycles and their cost",
     "Outdoor coils collect frost in humid cold weather. Defrost cycles briefly reverse the heat flow, which "
     "costs energy; smart defrost controls reduce the number of unnecessary cycles."),
    ("heat pumps in cold climates", "Grid impact of electrified heating",
     "Replacing fuel boilers with heat pumps raises winter electricity demand. Utilities plan for higher peaks on "
     "the coldest days and use demand response and better building insulation to soften them."),
    ("heat pumps in cold climates", "Limits and risks of heat pump retrofits",
     "Retrofits can disappoint when homes are poorly insulated or when units are undersized. Noise, refrigerant "
     "leaks and installer skill gaps are common risks that careful commissioning addresses."),
    ("solid-state batteries", "What makes a battery solid-state",
     "A solid-state battery replaces the liquid electrolyte of a lithium-ion cell with a solid one, often a "
     "ceramic, sulfide or polymer. The goal is higher energy density and lower fire risk."),
    ("solid-state batteries", "Lithium-metal anodes and dendrites",
     "Solid electrolytes could allow lithium-metal anodes, which store more energy. A key challenge is dendrites: "
     "metal filaments that grow through the electrolyte and short the cell."),
    ("solid-state batteries", "Manufacturing solid-state cells at scale",
     "Pressing thin solid layers without cracks and keeping good contact between them is hard to do at scale. "
     "Many lines need dry rooms and new equipment, which raises early production costs."),
    ("solid-state batteries", "Performance of solid-state batteries in cold weather",
     "Some solid electrolytes conduct ions poorly at low temperature, which limits charging speed in winter. "
     "Researchers test composite electrolytes to improve low-temperature performance."),
    ("solid-state batteries", "Safety case for solid electrolytes",
     "Removing flammable liquid electrolyte lowers the risk of thermal runaway, but sulfide electrolytes can "
     "release toxic gas when exposed to moisture, so safety depends on the chemistry."),
    ("solid-state batteries", "Roadmaps for solid-state batteries in vehicles",
     "Automakers describe pilot production first, then limited models, with wider use depending on cost and "
     "durability results. Timelines have slipped before, so roadmaps are best read as intentions."),
    ("durable execution for AI agents", "Why long-running agents need durable execution",
     "Agent workflows call slow and unreliable tools and models and may wait hours for people. If the process "
     "crashes, work is lost unless every step's result is persisted and the run can resume where it stopped."),
    ("durable execution for AI agents", "Event sourcing as the backbone of workflow engines",
     "Workflow engines record each step as an event in an append-only log. Replaying the log rebuilds the "
     "workflow state after a crash, and completed steps return their recorded results instead of running again."),
    ("durable execution for AI agents", "Retries with exponential backoff and jitter",
     "Transient failures are retried with exponentially growing delays. Adding random jitter spreads retries out "
     "so that many clients do not hit a recovering service at the same moment."),
    ("durable execution for AI agents", "Leases, heartbeats and fencing tokens",
     "A worker holds a lease on the run it executes and renews it with heartbeats. If it dies, the lease "
     "expires and another worker takes over; a fencing token stops a stalled worker from writing stale results."),
    ("durable execution for AI agents", "Human approval steps in agent workflows",
     "High-impact actions such as publishing or paying often need a person to approve, edit or reject them. The "
     "workflow pauses durably, records who decided, and escalates when nobody answers in time."),
    ("durable execution for AI agents", "Exactly-once effects through idempotency and outboxes",
     "Exactly-once execution is not achievable across a crash, but exactly-once effects are: write intended "
     "side effects to an outbox in the same transaction as the state change and deduplicate by idempotency key."),
    ("urban heat islands", "What causes urban heat islands",
     "Cities are warmer than their surroundings because dark surfaces absorb sunlight, buildings trap heat and "
     "there is less vegetation to cool the air through evaporation."),
    ("urban heat islands", "Health effects of urban heat",
     "Extreme heat in cities raises the risk of heat illness, especially for older people and those without "
     "cooling. Night-time heat is particularly harmful because the body cannot recover."),
    ("urban heat islands", "Cool roofs and reflective pavements",
     "Reflective roofs and pavements absorb less sunlight and lower surface temperatures. Their benefit depends "
     "on climate, building type and how surfaces age and get dirty over time."),
    ("urban heat islands", "Trees and green space as cooling infrastructure",
     "Street trees shade surfaces and cool the air through transpiration. Benefits are largest where canopy is "
     "continuous, and trees need water and maintenance to keep working."),
    ("urban heat islands", "Measuring urban heat with sensors and satellites",
     "Satellites map land surface temperature while street-level sensors measure the air temperature people "
     "feel. The two can differ, so cities combine both when planning interventions."),
    ("urban heat islands", "Equity and limits of heat mitigation",
     "Poorer neighbourhoods often have less tree cover and more heat. Mitigation plans can widen gaps if they "
     "ignore who benefits, and no single measure removes the heat island on its own."),
)

DOCUMENTS: tuple[Document, ...] = tuple(
    Document(
        doc_id=f"doc-{i:02d}",
        topic=topic,
        title=title,
        url="https://example.org/notes/" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-"),
        text=text,
    )
    for i, (topic, title, text) in enumerate(_NOTES, start=1)
)
TOPICS: tuple[str, ...] = tuple(dict.fromkeys(d.topic for d in DOCUMENTS))

_STOP = frozenset(
    "a an and are as at be by can does do for from how in is it its of on or the their them they this to "
    "what when where which who why with without about main next".split()
)
# Words the offline planner puts around every topic; they carry no information about the topic.
_QUERY_NOISE = frozenset(
    "background meant evidence studie study field data say limit limitation risk outlook changing".split()
)


def tokens(text: str) -> set[str]:
    """Lower-case word stems for keyword matching (a crude plural strip is enough here)."""
    out = set()
    for word in re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*", text.lower()):
        if len(word) < 3 or word in _STOP:
            continue
        out.add(word[:-1] if word.endswith("s") and len(word) > 4 else word)
    return out


def search(query: str, *, k: int = 3) -> list[Document]:
    """Top ``k`` documents by keyword overlap (title words count double).

    A document must share at least one topical word with the query; generic question words only
    break ties between documents on the same topic.
    """
    wanted = tokens(query)
    core = wanted - _QUERY_NOISE
    scored = []
    for doc in DOCUMENTS:
        title, body = tokens(doc.title), tokens(doc.text + " " + doc.topic)
        if not core & (title | body):
            continue
        scored.append((2 * len(wanted & title) + len(wanted & body), doc.doc_id, doc))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [doc for _, _, doc in scored[:k]]
