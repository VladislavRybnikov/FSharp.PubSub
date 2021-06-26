namespace FSharp.PubSub.Kafka

open System
open System.Threading
open Confluent.Kafka
open FSharp.Control.Reactive.Builders
open Newtonsoft.Json

module Sub =
    
    type Offset = string*int32*int64
    
    type Message<'T> = {
        TopicPartitionOffset: Offset
        Value: 'T
    }
    
    type OffsetCommitHandler = Offset -> unit
    
    type Subscription = {
        Topic: string
        CommitHandler: OffsetCommitHandler
    }
    
    type Consumer<'T> = string -> CancellationToken -> Subscription*IObservable<Message<'T>>
    
    type TopicConsumer<'T> = CancellationToken -> Subscription*IObservable<Message<'T>>
    
    let consumeF<'T> (consumer: IConsumer<string, string>) (topic: string) (ct: CancellationToken)=
        consumer.Subscribe topic
        
        let isEmpty = String.IsNullOrEmpty
        let msg (c: ConsumeResult<string, string>) = c.Message.Value
        let offset (c: ConsumeResult<string, string>): Offset = (c.Topic, c.Partition.Value, c.Offset.Value)
        
        let eventStream = observe {
           let mutable continueLooping = true
           
           while continueLooping do
               let consumeRes = consumer.Consume(ct)
               
               let message
                    = match consumeRes with
                       | x when not (x |> msg |> isEmpty) -> Some (msg x, offset x)
                       | _ -> None
                       |> Option.map(fun t -> {
                           TopicPartitionOffset = snd t
                           Value = JsonConvert.DeserializeObject<'T>(fst t)
                       })              
               if Option.isSome message then yield message.Value
              
               continueLooping <- not ct.IsCancellationRequested && not consumeRes.IsPartitionEOF 
               
        }
        
        let subscription = {
            Topic = topic
            CommitHandler = fun offsetData ->
                let topic, partition, offset = offsetData
                let tpo = TopicPartitionOffset(topic, Partition(partition), Offset(offset))
                consumer.Commit(seq { tpo })
        }
        
        (subscription, eventStream)
        
    let consumer<'T> (c: IConsumer<string, string>) = consumeF<'T> c
    
    let ofTopic<'T> (topic: string) (c: Consumer<'T>) = c topic
    
    let subscribe (ct: CancellationToken) (c: TopicConsumer<'T>) = c ct
        