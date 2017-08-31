import _acurl
import threading
import asyncio


class Session:
    def __init__(self, ae_loop, loop, ensure_running_func):
        self._ensure_running_func = ensure_running_func
        self._loop = loop
        self._session = _acurl.Session(ae_loop)

    async def request(self, url, event_loop=None):
        future = asyncio.futures.Future(loop=self._loop)
        self._session.request(url=url, user_object=future)
        self._ensure_running_func()
        return await future


class EventLoop:
    def __init__(self, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._ae_loop =  _acurl.EventLoop(self._complete)
        self._running = False

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

    def _scheduled_complete(self, completed):
        for complete in completed:
            complete.user_object.set_result(1)

    def _complete(self, completed):
        self._loop.call_soon_threadsafe(self._scheduled_complete, completed)

    def session(self):
        return  Session(self._ae_loop, self._loop, self.start_thread_if_needed)


