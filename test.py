import _acurl
import threading
import time
import sys

ev = _acurl.EventLoop()

def runner():
    ev.main()

thread = threading.Thread(target=runner)

N = 5000
P = 5

num = 0
failed = 0
def success():
    global num
    num += 1
    
def failure(x):
    global failed
    failed += 1
    
t1 = time.time()
for j in range(P):
    session = _acurl.Session(ev)
    for i in range(N//P):
        session.request('http://127.0.0.1:9003', success, failure)
t2 = time.time()
last_num = num
thread.start()
while num != N: 
    if last_num != num:
        last_num = num
        print(num, failed)
t3 = time.time()
print(t2 - t1, t3 - t2, file=sys.stderr)
print(N / (t3 - t2))
ev.stop()

