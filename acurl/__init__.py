import _acurl
import threading
import asyncio
import ujson
from collections import namedtuple

class RequestError(Exception):
    pass


Cookie = namedtuple('Cookie', 'domain flag path secure expiration name value'.split())


class Response:
    def __init__(self, resp):
        self._resp = resp
        self._prev = None
        self._body = None
        self._text = None
        self._headers = None
        self._encoding = None
        self._json = None

    @property
    def status_code(self):
        return self._resp.get_response_code()
    
    @property 
    def url(self):
        return self._resp.get_effective_url()

    @property
    def total_time(self):
        return self._resp.get_total_time()
    
    @property
    def namelookup_time(self):
        return self._resp.get_namelookup_time()

    @property
    def connect_time(self):
        return self._resp.get_connect_time()

    @property
    def appconnect_time(self):
        return self._resp.get_appconnect_time()

    @property
    def pretransfer_time(self):
        return self._resp.get_pretransfer_time()

    @property
    def starttransfer_time(self):
        return self._resp.get_starttransfer_time()

    @property
    def upload_size(self):
        return self._resp.get_size_upload()

    @property
    def download_size(self):
        return self._resp.get_size_download()

    @property
    def primary_ip(self):
        return self._resp.get_primary_ip()

    @property
    def cookielist(self):
        result = []
        for line in self._resp.get_cookielist():
            domain, flag, path, secure, expiration, name, value = line.split('\t')
            result.append(Cookie(domain, bool(flag), path, bool(secure), int(expiration), name, value))
        return result


    @property
    def history(self):
        result = []
        cur = self._prev
        while cur is not None:
            result.append(cur)
            cur = cur._prev
        result.reverse()
        return result

    @property
    def body(self):
        if self._body is None:
            self._body = b''.join(self._resp.get_body())
        return self._body
    
    @property
    def encoding(self):
        if self._encoding is None:
            if 'Content-Type' in self.headers and 'charset=' in self.headers['Content-Type']:
                self._encoding = self.headers['Content-Type'].split('charset=')[-1].split()[0]
            else:
                self._encoding = 'latin1'
        return self._encoding

    @encoding.setter
    def encoding_setter(self, encoding):
        self._encoding = encoding

    @property
    def text(self):
        if self._text is None:
            self._text = self.body.decode(self.encoding)
        return self._text

    @property
    def json(self):
        if self._json is None:
            self._json = ujson.loads(self.text)
        return self._json

    @property
    def headers(self):
        if self._headers is None:
            raw_header = b''.join(self._resp.get_header()).decode('ascii')
            self._headers = dict(l.split(': ', 1) for l in raw_header.split('\r\n')[1:-2])
        return self._headers


class Session:
    def __init__(self, ae_loop, loop):
        self._loop = loop
        self._session = _acurl.Session(ae_loop)

    async def get(self, url, **kwargs):
        return await self.request('GET', url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.request('PUT', url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request('POST', url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.request('DELETE', url, **kwargs)

    async def head(self, url, **kwargs):
        return await self.request('HEAD', url, **kwargs)

    async def options(self, url, **kwargs):
        return await self.request('OPTIONS', url, **kwargs)

    async def request(self, method, url, headers=None, cookies=None, auth=None, data=None, json=None, max_redirects=5):
        if headers is None:
            headers = {}
        if auth is not None:
            username, password = auth
            auth = ''.join([username, ':', password])
        if json is not None:
            if data is not None:
                raise ValueError('use only one or none of data or json')
            data = ujson.dumps(json)
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
        tuple_headers = tuple(headers.items())
        return await self._request(method, url, tuple_headers, cookies, auth, data, max_redirects)

    async def _request(self, method, url, headers, cookies, auth, data, remaining_redirects):
        future = asyncio.futures.Future(loop=self._loop)
        self._session.request(future, method, url, headers=headers, cookies=cookies, auth=auth, data=data)
        response = await future            
        redirect_url = response.get_redirect_url()
        if redirect_url is not None:
            if remaining_redirects == 0:
                raise RequestError('Max Redirects')
            redir_response = await self._request('GET', redirect_url, headers, cookies, auth, None, remaining_redirects - 1)
            redir_response._prev = Response(response)
            return redir_response
        else:
            return Response(response)


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
            self._thread = threading.Thread(target=self._runner, daemon=True)
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
        if error == None and response != None:
            future.set_result(response)
        elif error != None and response == None:
            future.set_exception(RequestError(error))
        

    def session(self):
        return  Session(self._ae_loop, self._loop)


