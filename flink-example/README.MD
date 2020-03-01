### Flink学习笔记
    flink中几乎所有的聚合操作 sum,reduce 都是针对KeyedStream 最大限度的利用分布式的并行处理能力.
    reduce((x,y)=>{
        //x 为上一次聚合的结果
        //y 为当前传入的数据
    })
    
#### Flink主要内容
* ###### Source
    输入数据源
* ###### Transformer
    针对数据源一些各种转换操作
* ###### Sink
    输出处理结果
* ###### Matermark 处理乱序数据
    Flink 可以通过设置 watermark 延迟窗口关闭触发时间,等待一定时间内的延迟数据.
periodic