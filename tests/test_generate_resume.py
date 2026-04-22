import importlib.util
import sys
import types
from pathlib import Path


def load_generate_resume_module():
    docx = types.ModuleType("docx")
    docx.Document = object
    sys.modules["docx"] = docx

    docx_oxml_ns = types.ModuleType("docx.oxml.ns")
    docx_oxml_ns.qn = lambda value: value
    sys.modules["docx.oxml.ns"] = docx_oxml_ns

    docx_shared = types.ModuleType("docx.shared")
    docx_shared.Inches = lambda value: value
    docx_shared.Pt = lambda value: value
    sys.modules["docx.shared"] = docx_shared

    docx_text_paragraph = types.ModuleType("docx.text.paragraph")
    docx_text_paragraph.Paragraph = object
    sys.modules["docx.text.paragraph"] = docx_text_paragraph

    docx_enum_text = types.ModuleType("docx.enum.text")
    docx_enum_text.WD_TAB_ALIGNMENT = object
    sys.modules["docx.enum.text"] = docx_enum_text

    script_path = Path(__file__).resolve().parent.parent / "scripts" / "generate-resume.py"
    spec = importlib.util.spec_from_file_location("generate_resume", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_direct_archetypes_preserve_requested_key() -> None:
    generate_resume = load_generate_resume_module()

    for key in [
        "gameplay",
        "mobile",
        "unreal",
        "graphics",
        "gamebackend",
        "backend",
        "fullstack",
        "frontend",
        "db",
        "multiplayer",
        "gameserver",
        "online",
        "liveops",
        "devops",
        "networking",
        "cpp",
        "vr",
        "sim",
        "genai",
        "cyber",
        "general",
    ]:
        assert generate_resume.normalize_requested_archetype(key) == key
