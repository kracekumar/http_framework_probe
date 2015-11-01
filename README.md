```
➜  http_framework_probe  wrk -c 5 -t 2 -s post.lua http://0.0.0.0:8080
Running 10s test @ http://0.0.0.0:8080
  2 threads and 5 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   224.51ms  415.16ms   1.92s    84.25%
    Req/Sec   164.90     68.93   222.00     81.43%
  1250 requests in 10.08s, 183.11KB read
  Non-2xx or 3xx responses: 1250
Requests/sec:    124.06
Transfer/sec:     18.17KB
```

```
➜  http_framework_probe  wrk -c 5 -t 2 -s post.lua http://0.0.0.0:8000
Running 10s test @ http://0.0.0.0:8000
  2 threads and 5 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   142.27ms   32.06ms 352.58ms   88.57%
    Req/Sec    14.03      5.23    20.00     76.24%
  280 requests in 10.09s, 67.27KB read
Requests/sec:     27.74
Transfer/sec:      6.67KB
```

