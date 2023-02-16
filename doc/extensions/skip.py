"""Skip members in Sphinx doc"""

from typing import Any

import sphinx
from sphinx.application import Sphinx


def skip_member(app: Sphinx, what, name, obj, skip, options):
    if name == "__init__":
        return True
    return skip


def setup(app: Sphinx) -> dict[str, Any]:
    app.connect("autodoc-skip-member", skip_member)
    return {
        "version": sphinx.__display_version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
