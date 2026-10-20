import importlib
import pkgutil
import sys
from typing import List, Optional

from hummingbot.analytics import commands


def main(argv: Optional[List[str]] = None) -> int:
    """Discover commands by module name so independent analytics features do not edit a shared registry."""
    argv = sys.argv[1:] if argv is None else argv
    names = sorted(module.name for module in pkgutil.iter_modules(commands.__path__) if not module.name.startswith("_"))
    if not argv or argv[0] not in names:
        print(f"usage: python -m hummingbot.analytics {{{','.join(names)}}} ...", file=sys.stderr)
        return 2
    return importlib.import_module(f"{commands.__name__}.{argv[0]}").main(argv[1:])


if __name__ == "__main__":
    sys.exit(main())
