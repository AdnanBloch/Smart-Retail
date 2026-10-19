from pathlib import Path

from app.core.config import Settings
from app.core.enums import ContentType
from app.v1.prompts.repo import Template, template_key, template_sources


def test_every_content_type_and_system_prompt_has_a_source():
    sources = template_sources(Settings(database_url="postgresql+asyncpg://x/y").prompts_dir)
    keys = {(ct, name) for ct, name, _ in sources}
    for ct in ContentType:
        assert (ct, "default") in keys
    for name in ("chat", "classifier", "merchant"):
        assert (None, name) in keys
    assert all(body for _, _, body in sources)


def test_template_key_and_render():
    assert template_key(Path("system.chat.md")) == (None, "chat")
    assert template_key(Path("faq.md")) == (ContentType.FAQ, "default")
    assert template_key(Path("README.md")) is None
    t = Template(ContentType.FAQ, "default", 1, "Tone $tone {braces} $missing")
    assert t.render(tone="calm") == "Tone calm {braces} $missing"
