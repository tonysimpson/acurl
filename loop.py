import time
import asyncio
import sys
import acurl

S = int(sys.argv[1])
N = int(sys.argv[2])

acurl_el = acurl.EventLoop()

async def test():
    session = acurl_el.session()
    done, pending = await asyncio.wait([session.request('http://127.0.0.1:9003') for i in range(N)])
    i = 0
    for f in done:
        try:
            f.result()
            i += 1
        except Exception as e:
            print('error:', repr(e))
    print(N / (time.time() - st), 'Errors:', N - i)
loop = asyncio.get_event_loop()
st = time.time()

def alive():
    print('*** PY ALIVE')
    loop.call_later(1, alive)

loop.call_later(1, alive)
loop.run_until_complete(asyncio.wait([test() for i in range(S)]))
print("TOTAL:", (S*N) / (time.time() - st))

