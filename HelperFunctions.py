from Settings import SIZE
import time
import socket
import sys
import threading

def requires_connection(func):
    """ initiates and cleans up connections with remote server """

    def inner(self, *args, **kwargs):
        self.mutex_.acquire()

        self.open_connection()
        ret = func(self, *args, **kwargs)
        self.close_connection()
        self.mutex_.release()

        return ret

    return inner


# reads from socket until "\r\n"
def read_from_socket(s):
    result = ""
    while 1:
        data = s.recv(256)
        if data[-2:] == "\r\n":
            result += data[:-2]
            break
        result += data
    #	if result != "":
    #		print "read : %s" % result
    return result


# sends all on socket, adding "\r\n"
def send_to_socket(s, msg):
    #	print "respond : %s" % msg
    s.sendall(str(msg) + "\r\n")

def lock_it_away():
    def decorator(func):
        def inner(self, *args, **kwargs):
            self.lock.acquire()
            ret = func(self, *args, **kwargs)
            self.lock.release()
            return ret
        return inner
    return decorator

def repeat_and_sleep(sleep_time):
    def decorator(func):
        def inner(self, *args, **kwargs):
            while True:
                time.sleep(sleep_time)
                if self.shutdown:
                    return
                func(self, *args, **kwargs)
                #if not ret:
                #    return
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
                    # exp retry time
                    time.sleep(2 ** retry_count)
                    retry_count += 1
            if retry_count == retry_limit:
                print("Target couldn't be reached. (Function " + str(func) + ", Thread " + str(threading.currentThread()) + ")")

                #self.close_connection()
                #self.unreachable = True
                #self.shutdown = True
                #sys.exit(-1)
        return inner
    return decorator


# Helper function to determine if a key falls within a range
def inrange(c, a, b):
    # is c in [a,b)?, if a == b then it assumes a full circle
    # on the DHT, so it returns True.
    a = a % SIZE
    b = b % SIZE
    c = c % SIZE
    if a < b:
        return a <= c and c < b
    return a <= c or c < b


class Address(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)

    def __hash__(self):
        return hash(("%s:%s" % (self.ip, self.port))) % SIZE

    def __cmp__(self, other):
        return other.__hash__() < self.__hash__()

    def __eq__(self, other):
        return other.__hash__() == self.__hash__()

    def __str__(self):
        return "[\"%s\", %s]" % (self.ip, self.port)
