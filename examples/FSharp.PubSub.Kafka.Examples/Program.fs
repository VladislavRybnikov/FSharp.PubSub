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
        |> Pub.ofProducer 
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
            |> Pub.ofProducer    
            |> Pub.forTopic "test-topic"
            |> Pub.withKey (fun t -> t.Id)
            
    playersProducer
        |> Pub.produce {
            Id = "2"
            Name = "Alice"
        }
        |> Async.RunSynchronously
        |> ignore
    
    0 