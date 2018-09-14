import signal


class ProcessManager(object):
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

    def listen(self, poll_func, exception_handler_func, cleanup_func):
        while True:
            try:
                poll_func()
            except Exception as e:
                exception_handler_func(e)
            if self.kill_now:
                break
        cleanup_func()
