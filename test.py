import _acurl
import threading
import time
import sys

def complete(completed):
    global num
    num += len(completed)
    print(completed[0].user_object)


ev = _acurl.EventLoop(complete)

def runner():
    ev.main()

thread = threading.Thread(target=runner)

N = 10000
P = 200

num = 0
failed = 0

t1 = time.time()
for j in range(P):
    session = _acurl.Session(ev)
    for i in range(N//P):
        session.request('http://127.0.0.1:9003', 22)
t2 = time.time()
last_num = num
thread.start()
while num != N: 
    if last_num != num:
        last_num = num
        print(num, failed)
        time.sleep(0.0001)
t3 = time.time()
print(t2 - t1, t3 - t2, file=sys.stderr)
print(N / (t3 - t2))
ev.stop()

