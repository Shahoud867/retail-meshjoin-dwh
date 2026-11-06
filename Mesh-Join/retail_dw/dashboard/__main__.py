"""Start the optional dashboard: python -m retail_dw.dashboard [--port 8765]"""

from __future__ import annotations

import argparse
import sys

from retail_dw.console import StartupAborted, prompt_connection
from retail_dw.dashboard.server import make_server


def main() -> int:
    parser = argparse.ArgumentParser(prog="retail_dw.dashboard", allow_abbrev=False)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        cfg = prompt_connection()
    except StartupAborted as exc:
        print(exc)
        return 1
    server = make_server(cfg, args.port)
    print(f"Dashboard: http://127.0.0.1:{server.server_address[1]}/  (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
