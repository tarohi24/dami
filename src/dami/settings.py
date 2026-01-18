from pathlib import Path
from typing import Final


PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent.parent
GCP_PROJECT: Final[str] = "strange-oxide-138404"
GS_BUCKET: Final[str] = "whiro-dami-storage"