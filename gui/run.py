"""
Entry point to launch the AI Essentials demo GUI.

Run from the `aie-demo` directory (parent of the demo_gui package):

    python -m demo_gui.run

Then open http://localhost:5001 in a browser.
"""
import os
import sys

# make the demo_gui package importable regardless of CWD
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from demo_gui.app import app  # noqa: E402


if __name__ == "__main__":
    port = int(os.environ.get("DEMO_GUI_PORT", "5001"))
    print(f"\n  AI Essentials demo GUI -> http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)
