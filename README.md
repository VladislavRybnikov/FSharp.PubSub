# FSharp.PubSub
![FSharp Logo](/images/logo.png)

Functional-first fsharp pub sub library.
FSharp.PubSub uses partial application fo functions to provide fluent builder-like interface for message queues publishing (Pub) and subscribing (Sub).
This concept abstract over concrete message queue and add on common way to use them.

# Kafka

### Pub :arrow_right:
Provides base method for publishing in kafka terminology

#### 1) Example (Full publishing pipeline): 

```fsharp
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
```

#### 2) Example (Creating preconfigured publisher for later usage):

```fsharp
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
```

### Sub :arrow_left:
- [ ] TODO
