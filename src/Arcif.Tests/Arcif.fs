
namespace Arcif.Tests
open System
open NUnit.Framework
open Arcif
open Connection
open Protocol

[<AutoOpen>]
module Helpers =
    let inline bindM builder m f = (^M: (member Bind: 'd -> ('e -> 'c) -> 'c) (builder, m, f))
    let inline (>>=) m f = bindM async m f
    let inline (>>.) m f = bindM async m (fun _ -> f)

    let rec print r =
        match r with
        | Status s -> s
        | Error e -> e
        | Integer i -> i.ToString()
        | Reply.Bulk s ->
            match s with
            | Some x -> x |> System.Text.Encoding.UTF8.GetString
            | None -> "Nil"
        | Reply.Multi m ->
            match m with 
            | Some m' -> 
                m' |> List.map print |> System.String.Concat
            | None -> "Nil"
        + "\r\n"

    let rec printReply r =
        async {
            print r |> printfn "%A"

            return r
         }

    let guid() = System.Guid.NewGuid().ToString() |> toBytes

    let run a = printReply a

module ArcifTests =
    [<Test>]
    let ``excercise the api`` () =
        use redis = Redis.connect "localhost" 6379
        async {
            redis.Select 2 >>= run >>.
            redis.FlushDb () >>= run >>.
            redis.Set "some_key"B "some value"B >>= run >>.
            redis.Set "some_key2"B "some value2"B >>= run >>.
            redis.Get "some_key2"B >>= run >>.
            redis.MGet ["some_key"B; "some_key2"B] >>= run >>.
            redis.LPush "my_list"B ["val1"B; "val2"B; "val3"B] >>= run >>.
            redis.LLen "my_list"B  >>= run >>.
            redis.LLen "not_my_list"B  >>= run >>.
            redis.LPop "my_list"B  >>= run >>.
            redis.LLen "my_list"B  >>= run >>.
            redis.LPop "not_my_list"B  >>= run >>.
            redis.Eval "return {1,2,{3,'Hello World!'}}" [] []  >>= run >>.
            redis.Multi() >>= run >>.
            redis.Set "multi"B "test"B >>= run >>.
            redis.Exec () >>= run >>.
            redis.Set "multi-line"B "first line \r\nsecond line\r\n"B >>= run >>.
            redis.Get "multi-line"B >>= run
            |> Async.RunSynchronously |> ignore
        }
        |> Async.RunSynchronously
