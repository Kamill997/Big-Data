import time
import threading

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=5, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.counter_failure = 0
        self.last_failure_timestamp = None
        self.state = 'CLOSED'
        self.lock = threading.Lock()

    def call(self, function, *args):
        with self.lock:
            if self.state == 'OPEN':
                time_since_failure = time.time() - self.last_failure_timestamp
                if time_since_failure > self.timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise CircuitBreakerOpenException("Circuit is open. Call denied.")

            try:
                print(f"Circuit is closed. Call ok.")
                result = function(*args)
            except self.expected_exception as e:
                self.counter_failure += 1
                self.last_failure_timestamp = time.time()
                if self.counter_failure >= self.failure_threshold:
                    self.state = 'OPEN'
                raise e
            else:
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                    self.counter_failure = 0
                if self.state== 'CLOSED':
                    self.counter_failure = 0

                return result

class CircuitBreakerOpenException(Exception):
    pass