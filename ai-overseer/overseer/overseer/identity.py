"""Identity & persona configuration.

The overseer has a name and a voice (in the literary sense). Both are
loaded from ``~/.overseer/identity.toml`` and injected as a system-prompt
prefix everywhere a model is invoked — agents, the planner, and the
conversation handler.

Defaults are written on first run so users can edit the persona without
having to know what fields exist.
"""
from __future__ import annotations

import shutil
import tomllib
from dataclasses import dataclass
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_PERSONA_RELATIVE = "personas/default.md"
DEFAULT_PERSONA_BUNDLED = PACKAGE_ROOT / "personas" / "default.md"


DEFAULT_IDENTITY_TOML = """\
# Overseer identity. Edit freely; reloaded on next start.

name = "Overseer"

# Path to the persona markdown. Relative paths resolve to this config dir.
persona_file = "personas/default.md"

# Placeholder for future TTS integration. Unused for now.
voice_id = ""
"""


@dataclass
class Identity:
    name: str
    persona_file: Path
    voice_id: str
    config_dir: Path

    def system_prompt_prefix(self) -> str:
        """Return the persona text with template variables filled in.

        Currently the only template variable is ``{name}``.
        """
        try:
            text = self.persona_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return f"You are {self.name}."
        return text.replace("{name}", self.name)


def _ensure_defaults(config_dir: Path) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    toml_path = config_dir / "identity.toml"
    if not toml_path.exists():
        toml_path.write_text(DEFAULT_IDENTITY_TOML)
    persona_dir = config_dir / "personas"
    persona_dir.mkdir(parents=True, exist_ok=True)
    persona_path = persona_dir / "default.md"
    if not persona_path.exists() and DEFAULT_PERSONA_BUNDLED.exists():
        shutil.copy(DEFAULT_PERSONA_BUNDLED, persona_path)


def load_identity(config_dir: Path | None = None) -> Identity:
    """Load (and on first run, materialize) the user's identity config.

    The persona file path in identity.toml may be relative; we resolve it
    against the config directory so users can ship multiple personas in
    ``~/.overseer/personas/`` and switch via the toml file.
    """
    config_dir = (config_dir or Path.home() / ".overseer").expanduser()
    _ensure_defaults(config_dir)

    toml_path = config_dir / "identity.toml"
    try:
        data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except (tomllib.TOMLDecodeError, FileNotFoundError):
        data = {}

    name = str(data.get("name", "Overseer"))
    persona_rel = str(data.get("persona_file", DEFAULT_PERSONA_RELATIVE))
    voice_id = str(data.get("voice_id", ""))

    persona_path = Path(persona_rel)
    if not persona_path.is_absolute():
        persona_path = (config_dir / persona_path).resolve()

    # If the configured persona doesn't exist, fall back to the bundled default
    # so callers always get something usable.
    if not persona_path.exists() and DEFAULT_PERSONA_BUNDLED.exists():
        persona_path = DEFAULT_PERSONA_BUNDLED

    return Identity(
        name=name,
        persona_file=persona_path,
        voice_id=voice_id,
        config_dir=config_dir,
    )
