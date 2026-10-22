"""Local HTTP entrypoint; use one worker for the V0.1 job manager."""

import argparse

import uvicorn
from dotenv import load_dotenv

from signaldesk.app import create_app


def main() -> None:
    # LangSmith reads os.environ directly, unlike the Pydantic settings loader.
    load_dotenv(override=False)
    parser = argparse.ArgumentParser(description="SignalDesk 竞品情报 Agent")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run(create_app(), host=args.host, port=args.port, workers=1)


if __name__ == "__main__":
    main()
