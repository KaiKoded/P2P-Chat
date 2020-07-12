import time
import socket
import threading


def repeat_and_sleep(sleep_time):
    def decorator(func):
        def inner(self, *args, **kwargs):
            while True:
                time.sleep(sleep_time)
                if self.shutdown_:
                    return
                func(self, *args, **kwargs)
        return inner
    return decorator


def retry_on_socket_error(retry_limit):
    def decorator(func):
        def inner(self, *args, **kwargs):
            retry_count = 0
            while retry_count < retry_limit:
                try:
                    ret = func(self, *args, **kwargs)
                    return ret
                except socket.error:
                    time.sleep(2 ** retry_count)
                    retry_count += 1
            if retry_count == retry_limit:
                print("Target couldn't be reached. (Function " + str(func) + ", Thread " + str(
                    threading.currentThread()) + ")")
        return inner
    return decorator
