import importlib.util
import sqlite3
from pathlib import Path


def load_sync_module():
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "db" / "sync_profile_archetypes.py"
    spec = importlib.util.spec_from_file_location("sync_profile_archetypes", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sync_profile_archetypes_inserts_missing_rows(tmp_path: Path) -> None:
    sync_module = load_sync_module()

    profile_path = tmp_path / "profile.yml"
    profile_path.write_text(
        """
target_roles:
  primary:
    - "gameplay"
    - "backend"

  archetypes:
    - name: "gameplay"
      level: "Mid"
      fit: "primary"
      interest: 8
      notes: "Gameplay focus."
    - name: "backend"
      level: "Mid"
      fit: "primary"
      interest: 8
      notes: "Backend focus."
    - name: "cpp"
      level: "Mid"
      fit: "adjacent"
      interest: 6
      notes: "Native systems exposure."
""".strip(),
        encoding="utf-8",
    )
    db_path = tmp_path / "job-log.db"

    inserted = sync_module.sync_archetypes(profile_path, db_path)

    assert inserted > 0
    with sqlite3.connect(db_path) as connection:
        profiles = [row[0] for row in connection.execute("SELECT profile_key FROM resume_profiles ORDER BY profile_key").fetchall()]
        configs = [row[0] for row in connection.execute("SELECT archetype_key FROM archetype_resume_configs ORDER BY archetype_key").fetchall()]
        letters = [row[0] for row in connection.execute("SELECT archetype FROM cover_letter_profiles ORDER BY archetype").fetchall()]
        rules = [row[0] for row in connection.execute("SELECT signal_key FROM resume_profile_signal_rules ORDER BY signal_key").fetchall()]
        archetype_labels = [row[0] for row in connection.execute("SELECT name FROM archetypes ORDER BY name").fetchall()]
        layout_rules = connection.execute(
            """
            SELECT profile_key, layout_key, is_default
            FROM resume_layout_profile_rules
            ORDER BY profile_key, layout_key
            """
        ).fetchall()

    assert profiles == ["backend", "cpp", "gameplay"]
    assert configs == ["backend", "cpp", "gameplay"]
    assert letters == ["backend", "cpp", "gameplay"]
    assert rules == ["archetype_backend", "archetype_cpp", "archetype_gameplay"]
    assert archetype_labels == ["Backend", "C++", "Gameplay"]
    assert layout_rules == [
        ("backend", "adjacent-1p", 0),
        ("backend", "dense-1p", 0),
        ("backend", "standard-1p", 1),
        ("backend", "standard-2p", 0),
        ("backend", "top-certs-1p", 0),
        ("backend", "top-certs-2p", 0),
        ("cpp", "adjacent-1p", 0),
        ("cpp", "dense-1p", 0),
        ("cpp", "standard-1p", 1),
        ("cpp", "standard-2p", 0),
        ("cpp", "top-certs-1p", 0),
        ("cpp", "top-certs-2p", 0),
        ("gameplay", "adjacent-1p", 0),
        ("gameplay", "dense-1p", 0),
        ("gameplay", "standard-1p", 1),
        ("gameplay", "standard-2p", 0),
        ("gameplay", "top-certs-1p", 0),
        ("gameplay", "top-certs-2p", 0),
    ]


def test_sync_signal_rules_inserts_only_missing_rules(tmp_path: Path) -> None:
    sync_module = load_sync_module()

    db_path = tmp_path / "job-log.db"
    profile_path = tmp_path / "profile.yml"
    profile_path.write_text(
        """
target_roles:
  archetypes:
    - name: "gameplay"
""".strip(),
        encoding="utf-8",
    )
    sync_module.sync_archetypes(profile_path, db_path)

    rules_path = tmp_path / "resume-signal-rules.yml"
    rules_path.write_text(
        """
rules:
  - profile_key: gameplay
    signal_key: jd_signal_gameplay
    operator: gte
    threshold_numeric: 2
    weight: 25
    action: score
    notes: "Boost gameplay profile."
  - profile_key: gameplay
    signal_key: jd_signal_gameplay
    operator: gte
    threshold_numeric: 2
    weight: 25
    action: score
    notes: "Duplicate should be ignored."
""".strip(),
        encoding="utf-8",
    )

    inserted = sync_module.sync_signal_rules(rules_path, db_path)
    inserted_again = sync_module.sync_signal_rules(rules_path, db_path)

    assert inserted == 1
    assert inserted_again == 0
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            "SELECT profile_key, signal_key, weight FROM resume_profile_signal_rules ORDER BY signal_key"
        ).fetchall()

    assert rows == [("gameplay", "archetype_gameplay", 70), ("gameplay", "jd_signal_gameplay", 25)]
