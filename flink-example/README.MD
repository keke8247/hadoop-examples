## Flink学习笔记
### Flink特点
* 事件驱动
* 流式数据(离线数据为有界数据/实时数据为无界数据)
    
        可以处理批量离线数据和实时的流式数据兼并了Storm和SparkStreaming的优点   
* 分层API
    
        顶层API 更抽象 含义简明 使用方便
        底层API 具体 丰富 功能颗粒度更细化 使用灵活
* 事件时间 常用的 EventTime ProcessTime
* 精确一致性状态保证 基于watermark 和 checkpoint机制 

#### Flink主要内容
* ###### Source
    输入数据源
* ###### Transformer
    针对数据源一些各种转换操作
* ###### Sink
    输出处理结果
* ###### Matermark 处理乱序数据
    Flink 可以通过设置 watermark 延迟窗口关闭触发时间,等待一定时间内的延迟数据.

