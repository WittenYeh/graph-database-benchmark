"""
Progress Server - Receives progress callbacks from containers
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import threading
from typing import Optional


class ProgressHandler(BaseHTTPRequestHandler):
    """HTTP handler for progress callbacks"""

    def log_message(self, format, *args):
        """Suppress default HTTP logging"""
        pass

    def do_POST(self):
        """Handle POST requests for progress updates"""
        if self.path != '/progress':
            self.send_response(404)
            self.end_headers()
            return

        try:
            # Read request body
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            data = json.loads(body.decode('utf-8'))

            # Process progress update
            event = data.get('event')
            if event == 'task_start':
                self._handle_task_start(data)
            elif event == 'task_complete':
                self._handle_task_complete(data)

            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')

        except Exception as e:
            print(f"⚠️  Error processing progress callback: {e}")
            sys.stdout.flush()
            self.send_response(500)
            self.end_headers()

    def _handle_task_start(self, data):
        """Handle task start event"""
        task_name = data.get('task_name')
        task_index = data.get('task_index')
        total_tasks = data.get('total_tasks')
        workload_file = data.get('workload_file')

        print(f"  ▶️  Task {task_index + 1}/{total_tasks}: {task_name} ({workload_file})")
        sys.stdout.flush()  # Force flush to ensure output is displayed

    def _handle_task_complete(self, data):
        """Handle task complete event"""
        task_name = data.get('task_name')
        task_index = data.get('task_index')
        total_tasks = data.get('total_tasks')
        duration = data.get('duration_seconds')
        status = data.get('status')

        status_icon = "✓" if status == "success" else "✗"
        print(f"  {status_icon}  Task {task_index + 1}/{total_tasks}: {task_name} completed in {duration:.2f}s ({status})")
        sys.stdout.flush()  # Force flush to ensure output is displayed


class ProgressServer:
    """HTTP server for receiving progress callbacks from containers"""

    def __init__(self, port: int = 8888):
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self):
        """Start the progress server in a background thread"""
        try:
            self.server = HTTPServer(('0.0.0.0', self.port), ProgressHandler)
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
            print(f"📡 Progress server started on port {self.port}")
        except Exception as e:
            print(f"⚠️  Failed to start progress server: {e}")
            self.server = None

    def stop(self):
        """Stop the progress server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join(timeout=2)
            print(f"📡 Progress server stopped")

    def get_callback_url(self) -> Optional[str]:
        """Get the callback URL for containers to use"""
        if self.server:
            # Use localhost since container runs in host network mode
            return f"http://localhost:{self.port}/progress"
        return None
