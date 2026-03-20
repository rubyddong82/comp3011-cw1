import http.client
import sys
import threading
import time

from client.client_main import main as client_main
from server.server_main import main as server_main


def wait_for_server(host: str, port: int, timeout: float = 8.0) -> None:
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=1.0)
            conn.request("GET", "/health")
            response = conn.getresponse()
            response.read()
            conn.close()

            if response.status == 200:
                return
        except Exception as exc:
            last_error = exc

        time.sleep(0.2)

    if last_error:
        raise RuntimeError(f"Server did not become ready: {last_error}")
    raise RuntimeError("Server did not become ready in time.")


def launch_server(host: str, port: int) -> None:
    server_main(host=host, port=port)


def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000

    server_thread = threading.Thread(
        target=launch_server,
        args=(host, port),
        daemon=True,
    )
    server_thread.start()

    wait_for_server(host, port)
    client_main(host=host, port=port)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down.")
