namespace FSharp.PubSub.Kafka

open Confluent.Kafka
open Newtonsoft.Json

module Pub =
    type ProduceResult = {
        Topic: string
        Partition: int
        Offset: int64
        Persisted: bool
    }
    
    type Producer<'T> = string -> ('T -> string) -> 'T -> Async<ProduceResult>
    type TopicProducer<'T> = ('T -> string) -> 'T -> Async<ProduceResult>
    type KeyedTopicProducer<'T> = 'T -> Async<ProduceResult>
    
    let produceF<'T> (producer: IProducer<string, string>) (topic: string) (key: 'T -> string) (value: 'T) =
        let msg = Message<string, string>(Key = key value, Value = JsonConvert.SerializeObject value)

        async {
            let! res = producer.ProduceAsync(topic, msg) |> Async.AwaitTask
            return {
                Topic = res.Topic
                Partition = res.Partition.Value
                Offset = res.Offset.Value
                Persisted = res.Status = PersistenceStatus.Persisted
            }
        }
        
    let ofProducer<'T> (p: IProducer<string, string>) = produceF<'T> p

    let forTopic<'T> (topic: string) (p: Producer<'T>) = p topic

    let withKey<'T> (key: 'T -> string) (p: TopicProducer<'T>) = p key
        
    let produce<'T> (value: 'T) (p: KeyedTopicProducer<'T>) = p value