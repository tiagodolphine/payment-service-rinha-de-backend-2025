from http.server import HTTPServer, SimpleHTTPRequestHandler
from json import dumps, loads
from logging import NOTSET, basicConfig, getLogger
from typing import Any

class Server(SimpleHTTPRequestHandler):
    """
    Summary
    -------
    a simple HTTP server that prints the request headers and body to the console

    Methods
    -------
    do_POST()
        prints the request headers and body to the console
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.logger = getLogger()
        super().__init__(*args, **kwargs)

    def do_POST(self):
        """
        Summary
        -------
        prints the request headers and body to the console
        """
        content_length = int(self.headers["Content-Length"])
        data = self.rfile.read(content_length).decode("utf-8")
        post_data_list = dumps(loads(data), indent=2).split("\n")
        formatted_data = "\n".join(line.strip() for line in post_data_list)

        self.logger.info("\nPOST\n%sBody:\n%s", self.headers, formatted_data)
        self.send_response(204)

def init_server() -> None:
    """
    Summary
    -------
    initializes the server
    """
    basicConfig(level=NOTSET, format="%(message)s")
    port = 5001
    httpd = HTTPServer(("localhost", port), Server)
    getLogger().info("Serving HTTP on :%d", port)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        getLogger().info("\rManual exit detected.")
    finally:
        httpd.server_close()
        getLogger().info("Shutting down..")

if __name__ == "__main__":
    init_server()