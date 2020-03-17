#### Redis 事务
    Multi 开启事务, exec 执行事务. 两条指令中间的命令 放在队列里面 顺序执行.
    如果有的语句语法错误,则整个事务都会失败.
    如果没有语法错误,执行的时候有问题 比如给 string +1 这样这条语句执行时候会失败. 事务里面其他的语句会执行成功.
    Watch 可以监控一个key的变化  如果监控期间Key的value发生改变.这放弃对Key操作的事务.
    watch机制类似ZK
#### Redis主从复制
    使用 slaveof ip port 命令 可以配置一个slaver节点.
    slaver配置以后 会清空自己原有的数据.把master的数据全量同步一下. slaver节点只能读 不能写
    如果 master节点挂了 slaver节点会等待.
    使用 slaveof no one 解除主从关系.解除关系以后 slaver节点保留了原有master的数据.
    slaver 挂了以后 主从关系解除.需要从新执行 slaveor ip port. 可以把这个命令配到配置文件中.
#### Redis 哨兵模式
    使用哨兵模式 可以监控redis的状态,master节点挂了 自动进行选主.快速恢复服务.
    sentinel monitor mymaster 127.0.0.1 6380 1
    配置 要监控的 master
    master 节点挂了 会从 slaver中选举一个新的master 接受写请求. 集群正常服务.
    原master 重新启动后 会自动变为slaver 加入集群.提供读服务.
 
    