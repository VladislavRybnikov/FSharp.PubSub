open System.Reactive.Linq
open System.Threading
open Confluent.Kafka
open FSharp.PubSub.Kafka

type PlayersTopic = {
        Id: string
        Name: string
    }

[<EntryPoint>]
let main argv =
    
    let producer = ProducerBuilder<string, string>(ProducerConfig()).Build()
        
    producer
        |> Pub.producer 
        |> Pub.forTopic "test-topic"
        |> Pub.withKey (fun t -> t.Id)
        |> Pub.produce {
            Id = "1"
            Name = "Bob"
        }
        |> Async.RunSynchronously
        |> ignore
    
    let playersProducer = // KeyedTopicProducer<'T> = 'T -> Async<ProduceResult>
            producer
            |> Pub.producer    
            |> Pub.forTopic "test-topic"
            |> Pub.withKey (fun t -> t.Id)
            
    playersProducer
        |> Pub.produce {
            Id = "2"
            Name = "Alice"
        }
        |> Async.RunSynchronously
        |> ignore
    
    let consumer = ConsumerBuilder<string, string>(ConsumerConfig()).Build()
    
    let subscription
        = consumer
        |> Sub.consumer<PlayersTopic>
        |> Sub.ofTopic "test-topic"
        |> Sub.subscribe CancellationToken.None
        
    snd subscription
        |> Observable.map(fun msg ->
            printfn $"Name = %s{msg.Value.Name}" 
            msg.TopicPartitionOffset
            )
        |> Observable.subscribe (fst subscription).CommitHandler
        |> ignore
    
    0 