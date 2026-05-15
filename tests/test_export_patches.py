# tests/test_export_patches.py
import sqlite3
import pytest
from pathlib import Path
from scripts.export_patches import export


@pytest.fixture
def tmp_db(tmp_path):
    db = tmp_path / "manifest.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE device_nodes (
            device_id TEXT, manufacturer TEXT, model TEXT,
            specs_json TEXT, patch_source TEXT, canonical_sku TEXT,
            canonical_product_name TEXT, queue INTEGER,
            stage_generate_patch INTEGER, stage_validate_patch INTEGER,
            device_class TEXT, batch_name TEXT
        )
    """)
    conn.execute("""
        INSERT INTO device_nodes VALUES
        ('yamaha-cl5','Yamaha','CL5','{"ports":[]}',
         'template CL5 { meta { kind: "device" manufacturer: "Yamaha" model: "CL5" } ports {} }',
         'yamaha-cl5', 'Yamaha CL5', 5, 2, 2, 'mixer', 'batch_200')
    """)
    conn.commit()
    conn.close()
    return db


def test_export_writes_to_manufacturer_subfolder(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    assert (out / "yamaha" / "yamaha-cl5.patch").exists()


def test_export_does_not_create_letter_shards(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    letter_dirs = [d for d in out.iterdir() if d.is_dir() and len(d.name) == 1]
    assert letter_dirs == []


def test_export_writes_quality_C_and_source_pipeline(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    content = (out / "yamaha" / "yamaha-cl5.patch").read_text()
    assert 'quality: "C"' in content
    assert 'source: "pipeline"' in content


def test_export_writes_batch_name(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    content = (out / "yamaha" / "yamaha-cl5.patch").read_text()
    assert 'batch: "batch_200"' in content


def test_export_writes_json_sidecar(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    assert (out / "yamaha" / "yamaha-cl5.json").exists()


def test_export_infers_manufacturer_from_slug(tmp_db, tmp_path):
    out = tmp_path / "patches"
    export(tmp_db, out, batch_name="batch_200")
    assert (out / "yamaha" / "yamaha-cl5.patch").exists()
    assert not (out / "y").exists()
