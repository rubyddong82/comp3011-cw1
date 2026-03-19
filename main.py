import sys
import threading
import time

from server.server_main import main as server_main
from client.client_main import main as client_main


def launch_server(host: str, port: int) -> None:
    server_main(host=host, port=port)


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python main.py <ip> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    server_thread = threading.Thread(
        target=launch_server,
        args=(host, port),
        daemon=True,
    )
    server_thread.start()

    time.sleep(1.0)

    client_main(host=host, port=port)


if __name__ == "__main__":
    main()