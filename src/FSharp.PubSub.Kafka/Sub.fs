namespace FSharp.PubSub.Kafka

module Sub =
    
    type ConsumeResult<'T> = {
        Topic: string
        Partition: int
        Offset: int64
        Message: 'T
    }