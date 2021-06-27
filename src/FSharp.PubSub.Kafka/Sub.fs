namespace FSharp.PubSub.Kafka

open System
open System.Threading
open Confluent.Kafka
open FSharp.Control.Reactive.Builders
open Newtonsoft.Json

module Sub =

    type Offset = string * int32 * int64

    type Msg<'T> internal (offset: Offset, value: 'T) =

        member this.Offset = offset

        member this.Value = value

        member val internal Consumer = null :> IConsumer<string, string> with get, set

    type TopicConsumeError =
        | SerializationError of string * string
        | KafkaError of string

    type TopicConsumeResult<'T> = Result<Msg<'T>, TopicConsumeError>

    type OffsetCommitHandler = Offset -> unit

    type Subscription =
        { Topic: string
          CommitHandler: OffsetCommitHandler }

    type Consumer<'T> = string -> CancellationToken -> IObservable<TopicConsumeResult<'T>>

    type TopicConsumer<'T> = CancellationToken -> IObservable<TopicConsumeResult<'T>>

    let consumeF<'T> (consumer: IConsumer<string, string>) (topic: string) (ct: CancellationToken) =
        consumer.Subscribe topic

        let isEmpty = String.IsNullOrEmpty
        let msg (c: ConsumeResult<string, string>) = c.Message.Value

        let withConsumer (c: IConsumer<string, string>) (m: Msg<'T>) =
            m.Consumer <- c
            m

        let offset (c: ConsumeResult<string, string>): Offset =
            (c.Topic, c.Partition.Value, c.Offset.Value)

        let tryParse (c: ConsumeResult<string, string>) =
            try
                Msg(offset c, JsonConvert.DeserializeObject<'T>(msg c))
                |> Some
                |> Result.Ok
            with ex ->
                TopicConsumeError.SerializationError(ex.Message, c.Message.Value)
                |> Result.Error

        observe {
            let mutable continueLooping = true

            while continueLooping do

                let consumed =
                    try
                        consumer.Consume(ct) |> Ok
                    with ex ->
                        (TopicConsumeError.KafkaError ex.Message)
                        |> Result.Error

                let consumeRes =
                    consumed
                    |> Result.bind (fun r ->
                        match r with
                        | x when not (x |> msg |> isEmpty) -> tryParse (x)
                        | _ -> None |> Result.Ok)

                match consumeRes with
                | Ok ok when ok.IsSome -> yield Ok(ok.Value |> withConsumer consumer)
                | Error e -> yield Result.Error e
                | _ -> ()

                let isEof =
                    match consumed with
                    | Ok ok -> ok.IsPartitionEOF
                    | _ -> false

                continueLooping <- not ct.IsCancellationRequested && not isEof

        }

    let consumer<'T> (c: IConsumer<string, string>) = consumeF<'T> c

    let ofTopic<'T> (topic: string) (c: Consumer<'T>) = c topic

    let asObservable<'T> (ct: CancellationToken) (c: TopicConsumer<'T>) = c ct

    let commit<'T> (m: Msg<'T>) =
        let topic, partition, offset = m.Offset
        m.Consumer.Commit(seq { TopicPartitionOffset(topic, Partition(partition), Offset(offset)) })