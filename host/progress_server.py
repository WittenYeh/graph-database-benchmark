"""
Progress Server - Receives progress callbacks from containers
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import threading
from typing import Optional, Callable, Dict, Any


class ProgressHandler(BaseHTTPRequestHandler):
    """HTTP handler for progress callbacks"""

    # Class variables
    restore_expected = False
    timeout_monitor = None  # Will be set by ProgressServer

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
            elif event == 'subtask_start':
                self._handle_subtask_start(data)
            elif event == 'subtask_complete':
                self._handle_subtask_complete(data)
            elif event == 'snapshot_start':
                self._handle_snapshot_start(data)
            elif event == 'snapshot_complete':
                self._handle_snapshot_complete(data)
            elif event == 'restore_start':
                self._handle_restore_start(data)
            elif event == 'restore_complete':
                self._handle_restore_complete(data)
            elif event == 'cleanup_start':
                self._handle_cleanup_start(data)
            elif event == 'cleanup_complete':
                self._handle_cleanup_complete(data)
            elif event == 'log_message':
                self._handle_log_message(data)
            elif event == 'error_message':
                self._handle_error_message(data)

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

    def _handle_subtask_start(self, data):
        """Handle subtask start event"""
        task_name = data.get('task_name')
        num_ops = data.get('num_ops', 0)

        # Check if restore was expected but didn't happen
        if ProgressHandler.restore_expected:
            print(f"    ⚠️  Warning: Expected restore before subtask start")
        print(f"    ▶️  {task_name}")
        sys.stdout.flush()

        # Reset restore expectation when subtask starts
        ProgressHandler.restore_expected = False

        # Start timeout monitor if available
        if ProgressHandler.timeout_monitor and num_ops > 0:
            ProgressHandler.timeout_monitor.start_subtask_timer(data)

    def _handle_subtask_complete(self, data):
        """Handle subtask complete event"""
        task_name = data.get('task_name')
        duration = data.get('duration_seconds')
        status = data.get('status')
        original_ops = data.get('original_ops_count')
        valid_ops = data.get('valid_ops_count')
        filtered_ops = data.get('filtered_ops_count')

        status_icon = "✓" if status == "success" else "✗"

        # Build output message
        msg = f"    {status_icon}  {task_name} completed in {duration:.2f}s"

        # Add operation counts if available
        if original_ops is not None and valid_ops is not None:
            if filtered_ops and filtered_ops > 0:
                msg += f" [{valid_ops}/{original_ops} valid ops, {filtered_ops} filtered]"
            else:
                msg += f" [{valid_ops} valid ops / {original_ops} total ops]"
            msg += " (latency is for valid ops only)"

        print(msg)
        sys.stdout.flush()

        # Cancel timeout monitor if available
        if ProgressHandler.timeout_monitor:
            ProgressHandler.timeout_monitor.cancel_subtask_timer()

        # After subtask completes, expect restore before next subtask
        ProgressHandler.restore_expected = True
    def _handle_snapshot_start(self, data):
        """Handle snapshot start event"""
        print(f"  📸 Creating database snapshot...")
        sys.stdout.flush()

    def _handle_snapshot_complete(self, data):
        """Handle snapshot complete event"""
        status = data.get('status')
        status_icon = "✓" if status == "success" else "✗"
        print(f"  {status_icon}  Snapshot created successfully")
        sys.stdout.flush()

    def _handle_restore_start(self, data):
        """Handle restore start event - suppress normal output"""
        pass

    def _handle_restore_complete(self, data):
        """Handle restore complete event - only show errors"""
        status = data.get('status')
        if status != 'success':
            print(f"  ❌ Failed to restore database from snapshot")
            sys.stdout.flush()
        else:
            # Clear restore expectation after successful restore
            ProgressHandler.restore_expected = False

    def _handle_cleanup_start(self, data):
        """Handle cleanup start event"""
        print(f"  🧹 Cleaning up database...")
        sys.stdout.flush()

    def _handle_cleanup_complete(self, data):
        """Handle cleanup complete event"""
        status = data.get('status')
        status_icon = "✓" if status == "success" else "✗"
        print(f"  {status_icon}  Database cleanup completed")
        sys.stdout.flush()

    def _handle_log_message(self, data):
        """Handle log message event"""
        message = data.get('message', '')
        level = data.get('level', 'INFO')
        if level == 'INFO':
            print(f"  ℹ️  {message}")
        elif level == 'WARNING':
            print(f"  ⚠️  {message}")
        sys.stdout.flush()

    def _handle_error_message(self, data):
        """Handle error message event"""
        message = data.get('message', '')
        error_type = data.get('error_type', 'Error')
        print(f"  ❌ {error_type}: {message}")
        sys.stdout.flush()


class ProgressServer:
    """HTTP server for receiving progress callbacks from containers"""

    def __init__(self, port: int = 8888, timeout_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.thread: Optional[threading.Thread] = None
        self.timeout_callback = timeout_callback

    def start(self):
        """Start the progress server in a background thread"""
        # Import here to avoid circular dependency
        from timeout_monitor import TimeoutMonitor

        # Initialize timeout monitor if callback provided
        if self.timeout_callback:
            ProgressHandler.timeout_monitor = TimeoutMonitor(self.timeout_callback)

        for port in range(self.port, self.port + 10):
            try:
                self.server = HTTPServer(('0.0.0.0', port), ProgressHandler)
                self.port = port
                self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
                self.thread.start()
                print(f"📡 Progress server started on port {self.port}")
                return
            except OSError:
                continue
        print(f"⚠️  Failed to start progress server: no available port in range {self.port}-{self.port + 9}")
        self.server = None

    def stop(self):
        """Stop the progress server"""
        # Shutdown timeout monitor
        if ProgressHandler.timeout_monitor:
            ProgressHandler.timeout_monitor.shutdown()
            ProgressHandler.timeout_monitor = None

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
