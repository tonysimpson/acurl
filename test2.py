import acurl
import asyncio

el = acurl.EventLoop()
s = el.session()
r = asyncio.get_event_loop().run_until_complete(s.request('GET', 'https://httpbin.org/ip'))
print(r)
print(r.status_code)
print(r.headers)
print(r.json)
el.stop()
