"""
Seed DB with sample events for testing the integrated pipeline + WebDB.
"""
import os, sys, random, json
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.db.schema import init_db_sync, seed_fault_types
from src.db.models import EventCreate
from src.db.repository import create_event
from src.db.similarity import update_similarity_links
from src.import_job import CASE_TO_FAULT


def seed_db():
    conn = init_db_sync()
    seed_fault_types(conn)
    # Add Unknown fault type if not exists
    conn.execute("INSERT OR IGNORE INTO fault_types (name, description) VALUES ('Unknown', 'Unclassified')")
    conn.commit()

    BEAM_TIMES = [
        "2024-1st", "2024-1st MS", "2024-2nd", "2024-2nd MS",
        "2025-1st", "2025-1st MS", "2025-2nd", "2025-2nd MS",
        "2026-1st", "2026-1st MS",
    ]

    # Ensure all fault types from CASE_TO_FAULT exist in fault_types
    for case_id, ft_name in CASE_TO_FAULT.items():
        if ft_name == "Unknown":
            continue
        conn.execute(
            "INSERT OR IGNORE INTO fault_types (name, description) VALUES (?, ?)",
            (ft_name, f"Auto-classified case {case_id}"),
        )
    conn.commit()

    count = 0
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    for bt_idx, bt in enumerate(BEAM_TIMES):
        n = 4 if bt_idx % 2 == 0 else 3
        for i in range(n):
            # Distribute cases: digital cases first, then analog, then beam/unknown
            if i < 2:
                case_id = [3, 4, 5, 6, 7][(bt_idx + i) % 5]
            elif i < 3:
                case_id = [8, 9, 10, 11, 12, 13][(bt_idx + i) % 6]
            else:
                case_id = [0, 1, 2][(bt_idx + i) % 3]

            ft_name = CASE_TO_FAULT.get(case_id, "Unknown")

            evt_time = base + timedelta(days=bt_idx * 30 + i * 10, hours=10 + i * 3)
            evt_id = evt_time.strftime("%Y%m%d_%H%M%S") + f"_S{bt_idx:02d}{i:02d}"
            ts_iso = evt_time.strftime("%Y-%m-%dT%H:%M:%S")

            dig_patterns = {
                3: {"INT_IC_FC1": 1, "INT_MIS1_IC": 0},
                4: {"INT_MIS1_IC": 1, "INT_MIS2_IC": 1},
                5: {"INT_PSI1_IC": 1, "INT_PSI2_IC": 0},
                6: {"RDY_KSU1_IC": 1, "RDY_KSU2_IC": 1, "RDY_KSU3_IC": 1},
                7: {"INT_MIS1_IC": 1, "INT_PSI1_IC": 1},
            }
            dig = dig_patterns.get(case_id, {})
            analog = {
                "Forward_SRF1": {"peak": round(1.0 + random.random(), 2)},
                "Cavity_SRF1": {"peak": round(0.5 + random.random(), 2)},
            }

            event = EventCreate(
                id=evt_id,
                timestamp=ts_iso,
                merged_file=f"data/merged/event_{evt_id}.parquet",
                fault_type=ft_name,
                fault_confidence=round(0.7 + case_id / 13 * 0.25, 2),
                beam_voltage=round(2.0 + (bt_idx % 3) * 0.5, 1),
                beam_current=round(100 + i * 30 + bt_idx * 10, 1),
                analog_metrics=analog,
                digital_pattern=dig,
                time_groups={"FIRST": 2, "SECOND": 1, "THIRD": 0},
                case_id=case_id,
                case_description=f"Case {case_id} auto-classified",
                case_fault=ft_name,
                user_beam_time=bt,
                report_md=f"# Event: {ft_name}\n- Time: {ts_iso}\n- Case: {case_id}\n",
            )

            try:
                create_event(conn, event)
                update_similarity_links(conn, event.id)
                count += 1
            except Exception as e:
                print(f"  Error {evt_id}: {e}")

    conn.close()
    print(f"✅ Seeded {count} events")
    return count


if __name__ == "__main__":
    seed_db()
