open System.Threading
open Confluent.Kafka
open FSharp.Control.Reactive
open FSharp.PubSub.Kafka
open FSharp.Control.Reactive.Builders

type PlayersTopic = {
        Id: string
        Name: string
    }

let testProducer () =
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
    
let consumerTest() =
    let consumer = ConsumerBuilder<string, string>(ConsumerConfig()).Build()
    
    consumer
        |> Sub.consumer<PlayersTopic>
        |> Sub.ofTopic "test-topic"
        |> Sub.asObservable CancellationToken.None
        |> Observable.map(fun msg ->
            match msg with
            | Ok ok -> 
                printfn $"Name = %s{ok.Value.Name}" 
                ok |> Some
            | _ -> None
            )
        |> Observable.subscribe (fun opt ->
            match opt with
            | Some m -> Sub.commit m
            | _ -> ())

[<EntryPoint>]
let main argv =
    
    let observable = observe {
        yield 1
        printfn "1"
        
        yield 2
        printfn "2"
    }
    
    observable
    |> Observable.subscribe (fun x -> printfn $"Consumed: {x}")
    |> Disposable.dispose
    
    0