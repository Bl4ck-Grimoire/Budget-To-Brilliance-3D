"""SYNTHETIC mini Data Warehouse used ONLY by the automated tests (never for evidence or demos).

It has the exact shape of the real DW (create_dw.sql) with invented rows, so the producer SQL, the
consumer and the views can be exercised without the 3.29M-row load. Built-in effects (so tests can
assert them): NO OFICIAL scores higher than OFICIAL, URBANO higher than RURAL, M higher than F in
every subject, and dropout negatively related to the department mean score.
"""
from __future__ import annotations

import random

from src import config

GENDERS = [(1, "F", "Femenino"), (2, "M", "Masculino"), (-1, "No informado", "No informado")]
PERIODS = [(1, "2024-2", 2024, 2), (2, "2025-2", 2025, 2)]


def seed(conn, n_2024: int = 2400, n_2025: int = 300, seed_value: int = 7) -> dict:
    rng = random.Random(seed_value)
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in ["fact_resultado_saber11", "fact_indicador_departamento", "dim_colegio", "dim_periodo",
              "dim_genero", "dim_anio", "dim_departamento"]:
        cur.execute(f"TRUNCATE TABLE {t}")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    depts = [(-1, "00", "No aplica")] + [(i + 1, c, n) for i, (c, n) in enumerate(sorted(config.DANE_DEPARTAMENTOS.items()))]
    cur.executemany("INSERT INTO dim_departamento VALUES (%s, %s, %s)", depts)
    cur.executemany("INSERT INTO dim_anio VALUES (%s, %s)", [(2022, 2022), (2023, 2023), (2024, 2024), (2025, 2025)])
    cur.executemany("INSERT INTO dim_genero VALUES (%s, %s, %s)", GENDERS)
    cur.executemany("INSERT INTO dim_periodo VALUES (%s, %s, %s, %s, %s)", [(i, p, a, s, a) for i, p, a, s in PERIODS])

    # three schools per department + the -1 'validante' member
    cur.execute("INSERT INTO dim_colegio VALUES (-1, 'NO_APLICA', 'No aplica', 'No aplica', 'No aplica', 'No aplica', 'No aplica', 'No aplica', -1)")
    schools = {}
    cid = 1
    for dep_id, cod, name in depts[1:]:
        for kind, nat, area in [("a", "OFICIAL", "URBANO"), ("b", "OFICIAL", "RURAL"), ("c", "NO OFICIAL", "URBANO")]:
            cur.execute("INSERT INTO dim_colegio VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (cid, f"{cod}{kind}", f"Colegio {cod}{kind}", nat, "COMPLETA", area, name, "X", dep_id))
            schools.setdefault(cod, []).append((cid, nat, area))
            cid += 1

    # MEN indicators 2022-2024 only (the real batch window); dropout falls as the dept effect rises
    dept_effect = {cod: rng.uniform(-15, 15) for cod in config.DANE_DEPARTAMENTOS}
    for dep_id, cod, _ in depts[1:]:
        for anio in (2022, 2023, 2024):
            cur.execute("INSERT INTO fact_indicador_departamento VALUES (%s, %s, %s, %s)",
                        (dep_id, anio, round(6 - dept_effect[cod] / 4 + rng.uniform(-.3, .3), 2),
                         round(5 - dept_effect[cod] / 5 + rng.uniform(-.3, .3), 2)))

    def score(base: float, bonus: float) -> float:
        return round(min(100.0, max(1.0, rng.gauss(base + bonus, 8))), 1)

    rows = []
    for id_periodo, periodo, n in [(1, "2024-2", n_2024), (2, "2025-2", n_2025)]:
        for k in range(n):
            validante = rng.random() < 0.05
            cod = rng.choice(list(config.DANE_DEPARTAMENTOS))
            gender = rng.choice([1, 2, 1, 2, 2, -1])
            if validante:
                cid_, nat, area = -1, "No aplica", "No aplica"
            else:
                cid_, nat, area = rng.choice(schools[cod])
            bonus = dept_effect[cod] + (4 if nat == "NO OFICIAL" else 0) + (3 if area == "URBANO" else 0) + (2 if gender == 2 else 0)
            subj = [score(50, bonus) for _ in range(5)]
            glob = round(sum(subj) / 5 * 5, 0)
            glob = float(min(500, max(0, glob)))
            suspicious = rng.random() < 0.01
            if suspicious:
                glob = 0.0
            rows.append((f"SB11{periodo.replace('-', '')}{k:07d}", cid_, gender, id_periodo, *subj, glob, suspicious))
    cur.executemany("INSERT INTO fact_resultado_saber11 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
    conn.commit()
    return {"rows": len(rows), "dept_effect": dept_effect}
