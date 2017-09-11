import asyncio
import sys
import time
import acurl


def duration_generator(thing, duration):
    end_t = time.time() + duration
    while time.time() < end_t:
        yield thing


async def runner(acurl_el, requests):
    session = acurl_el.session()
    i = 0
    for request in requests:
        await session.request(request)
        i += 1
    return i


async def group(number, duration):
    acurl_el = acurl.EventLoop()
    requests = duration_generator('http://localhost:9003', duration)
    results = await asyncio.gather(*[runner(acurl_el, requests) for i in range(number)])
    acurl_el.stop()
    return sum(results)


def main(number, duration):
    count = asyncio.get_event_loop().run_until_complete(group(number, duration))
    print('Count:', count)
    print('TPS:', count / duration)
    
  
if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))

