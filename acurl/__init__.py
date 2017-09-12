import _acurl
import threading
import asyncio


class Session:
    def __init__(self, ae_loop, loop):
        self._loop = loop
        self._session = _acurl.Session(ae_loop)

    async def request(self, url):
        future = asyncio.futures.Future(loop=self._loop)
        self._session.request(url=url, user_object=future)
        return await future


class EventLoop:
    def __init__(self, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._ae_loop =  _acurl.EventLoop()
        self._running = False
        self._loop.add_reader(self._ae_loop.get_out_fd(), self._complete)
        self.start_thread_if_needed()

    def start_thread_if_needed(self):
        #print("start_thread_if_needed: running={}".format(self._running))
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._runner)
            self._thread.start()

    def _runner(self):
        self._ae_loop.main()
        #print("runner: ae loop finished")
        self._running = False

    def stop(self):
        if self._running:
            self._ae_loop.stop()

    def __del__(self):
        self.stop()

    def _complete(self):
        error, response, future = self._ae_loop.get_completed()
        future.set_result(response)

    def session(self):
        return  Session(self._ae_loop, self._loop)


