"""
TimeoutMonitor - Monitors subtask execution and handles timeouts
"""
import threading
import time
from typing import Optional, Callable, Dict, Any


class TimeoutMonitor:
    """
    Monitors subtask execution and triggers timeout handling.

    When a subtask starts, begins a timer based on num_ops * 10000us.
    If the subtask doesn't complete within this time, marks it as failed
    and triggers container restart.
    """

    def __init__(self, timeout_callback: Callable[[Dict[str, Any]], None]):
        """
        Initialize timeout monitor.

        Args:
            timeout_callback: Function to call when timeout occurs.
                             Receives subtask data as parameter.
        """
        self.timeout_callback = timeout_callback
        self.current_timer: Optional[threading.Timer] = None
        self.current_subtask: Optional[Dict[str, Any]] = None
        self.lock = threading.Lock()

    def start_subtask_timer(self, subtask_data: Dict[str, Any]):
        """
        Start timeout timer for a subtask.

        Args:
            subtask_data: Subtask information including num_ops
        """
        with self.lock:
            # Cancel any existing timer
            if self.current_timer:
                self.current_timer.cancel()

            # Calculate timeout: num_ops * 10000us = num_ops * 0.01s
            num_ops = subtask_data.get('num_ops', 0)
            timeout_seconds = num_ops * 0.01  # 10000us = 0.01s

            # Store current subtask info
            self.current_subtask = subtask_data.copy()
            self.current_subtask['timeout_seconds'] = timeout_seconds
            self.current_subtask['start_time'] = time.time()

            # Create and start timer
            self.current_timer = threading.Timer(
                timeout_seconds,
                self._handle_timeout
            )
            self.current_timer.daemon = True
            self.current_timer.start()

    def cancel_subtask_timer(self):
        """Cancel the current subtask timer (called when subtask completes)."""
        with self.lock:
            if self.current_timer:
                self.current_timer.cancel()
                self.current_timer = None
            self.current_subtask = None

    def _handle_timeout(self):
        """Internal method called when timeout occurs."""
        with self.lock:
            if self.current_subtask:
                elapsed = time.time() - self.current_subtask['start_time']
                print(f"    ⏰ TIMEOUT: Subtask exceeded {self.current_subtask['timeout_seconds']:.2f}s "
                      f"(elapsed: {elapsed:.2f}s)")

                # Call the timeout callback
                self.timeout_callback(self.current_subtask)

                # Clear current subtask
                self.current_subtask = None
                self.current_timer = None

    def shutdown(self):
        """Shutdown the monitor and cancel any pending timers."""
        with self.lock:
            if self.current_timer:
                self.current_timer.cancel()
                self.current_timer = None
            self.current_subtask = None
