#-*-coding:utf-8-*-
redis_nodes=['127.0.0.1:6580','127.0.0.1:6581']#[host:port]
zkServers='127.0.0.1:2181'
zkTimeoutSec=10
zkRootPath='/RedisNodes'
redisCli='/usr/local/bin/redis-cli'
checkIntervalSec=2
checkTimeoutSec=5

RoleName="Redis_"
Policy=""
SiteName=""
Weight="1000"
RouteValue="host=%s\r\nport=%d\r\nmaxWait=0\r\nrequestTimeout=1000"


