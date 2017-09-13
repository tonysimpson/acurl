import acurl
import asyncio

el = acurl.EventLoop()
s = el.session()
r = asyncio.get_event_loop().run_until_complete(s.request('GET', 'http://google.com'))
print(r)
print(b''.join(r.get_raw()).decode('utf8'))
el.stop()
