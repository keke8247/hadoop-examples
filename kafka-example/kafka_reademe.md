#kafka学习笔记#
###kafka简介###
####基于订阅发布模式的分布式消息队列.主要有以下特点:####
* 消息持久化 默认存储7天的消息数据 以数据log文件的形式存储
* 高吞吐量  topic可以分为多个partition,提升负载的同时,也提高了写数据和读数据的并发.采用顺序写模式 和 零拷贝技术.
* 分布式易扩展 使用zk管理集群,添加一个节点只需要把服务器连到对应的zk集群即可加入服务.
* 多客户端语言支持.

####集群运行基本组件包含:####
* Broker 集群中每一台机器称为一个Broker,一个kafka集群包含多个broker
* Producer 消息生产者
* Consumer 消息消费者,消费消息的时候以 consumer group的形式.
* Topic 消息主题 逻辑概念
* Partition 消息分区 物理概念以文件夹的形式存在.

####主要组件功能模块分析####
##### 1.broker #####
    接收存储消息.每个broker存储一个或者多个partition分区.由于引入了partition分区,kafka只能保证partition内部消息的顺序性,不能保证消息的全局顺序性.
##### 2.topic #####
    消息主题,consumer订阅消息的时候就是以topic的形式来订阅的.
    在创建topic的时候,可以指定topic的分区个数(--partitions 2 表示2个分区).以及副本数(--replication-factor 2 表示两个副本),
    副本数不大于集群中broker的个数,因为不同的副本存放在不同的broker上.这里的副本有Leader和Follwer之分.Follwer只是用来做备份.
    在Leader出现故障之后 可以被选为Leader继续提供服务.选举Leader的时候有个ISR机制,在主从数据一致性方面有两种方案
        1:过半Follwer同步完成视为同步完成(过半机制)
        2:全量的Follwer同步完成视为同步完成(强一致性,延迟最大.)
    kafka选用了第2种方案,不过在此基础上做了一定程度的优化,通过把和Leader同步时间误差小于某一个值的Follwer归入一个ISR集合,
    后续选Leader从ISR集合中选出.写数据的时候 当ack=-1 也只需要等待ISR集合中的所有Follwer同步完成就返回ack响应.以此提高效率.
    
##### 3.producer #####
    消息生产者,生产消息的时候需要指定topic,可以指定具体的分区,如果没有指定分区则按照Key的hash和可用分区数取模得到分区.
    如果既没有指定分区也没有指定key,会随机生成一个值然后去hash和可用分区数取模,后续再有没有指定分区和key的情况下在此随机数上累加取hash.
    producer在投递消息的时候通过ack级别设定来保证消息成功投递.ack有三个级别:
        1. ack=0 消息投递出去不需要等待leader的ack响应.继续下一轮的投递. 这样会有消息丢失的风险.
        2. ack=1 消息投递出去,只要leader写入成功立即返回ack响应.如果在没有通知Follwer同步数据的时候leader挂了,同样会丢失数据.
        3. ack=-1 消息投递出去,需要等待ISR集合里所有副本节点都同步完成,才返回ack响应(如果ISR集合里只有一个副本节点,则退化为ack=1的情况了)
            ack=-1 消息投递安全级别最高.延迟也最大.

##### 4.consumer #####
    消息消费者,在消费消息的时候,以Consumer Group消费者组的形式消费消息,维护offset.kafka 0.9版本以前(含)offset是维护在zk上的.0.9版本以后可以维护在kafka集群.
    由于offset是经常改变的,维护在zk集群,大量的consumer消费消息都会和zk通信修改offset,会增加zk的负担.offset维护在kafka集群是以 GTP(消费者组/topic/partition)
    所以消费者在消费消息的时候,只有通过消费者组的形式 结合订阅的topic和partition才能访问到offset,进行消息的消费. 
    同一时刻一个partition的消息只能被同一消费者组中的一个消费者消费.(不同消费者组没这个限制.)
    consumer在消费消息的时候有两种不同的分发策略:
        1.RoundRobin循环轮训, 这种策略 kafka把所有topic的所有partition作为一个总体,然后从挨个轮训分发给不同的consumer.这样在 同个消费者组中不同consumer
        订阅了不同主题的情况下会把本没有订阅的topic的消息分发到同组其他的consumer上. 
        2.Range 先根据consumer订阅的topic区分.再把不同的partition分给consumer,这样可能会有数据消费倾斜的问题.kafka默认采用的这种策略.
        
##### 5.partition #####
    消息分区,是一个物理上以文件夹的形式存在的.是kafka最小的并行单元.

#### kafka的数据一致性保证 ####
##### 消费一致性 #####
    kafka引入了HW(high watermark)高水位线机制,保证消费一致性. 把所有副本中保留的最小的offset作为对consumer可见的最大offset,从而保证无论哪个Follwer被
    选为Leader,consumer消费的数据都存在.
    
##### 存储一致性 #####
    在HW的基础,kafka结合LEO(log end offset),在有新的Follwer选为Leader后,除Leader外所有Follwer从HW位置截取.然后和Leader的offset作比较,多退少补进行
    数据同步.保证存储一致性.
