import acurl
import asyncio

el = acurl.EventLoop()
s = el.session()
print(asyncio.get_event_loop().run_until_complete(s.request('http://localhost:9003')))
