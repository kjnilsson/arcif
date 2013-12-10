
// NOTE: If warnings appear, you may need to retarget this project to .NET 4.0. Show the Solution
// Pad, right-click on the project node, choose 'Options --> Build --> General' and change the target
// framework to .NET 4.0 or .NET 4.5.

module Arcif.Integration.Main

open System

open Arcif.Redis
open Arcif.Redis.Strings

open Arcif.Redis.Server

let asBytes (s:string) =
    System.Text.Encoding.UTF8.GetBytes s

let check test target value =
    let result = 
        value
        |> Async.RunSynchronously
        |> function
            | Status x' -> x' |> asBytes = target
            | Reply.Bulk (Some x') ->  x' = target
            | _ -> false

    if result then
        printfn  "test: %s is OK" test
    else
        printfn "!! test: %s failed !!" test


[<EntryPoint>]
let main args = 
    Console.WriteLine("Arcif Integration Tests")
    
    use r = Redis.connect "localhost" 6379

    r.Set "name"B "karl-johan"B |> Async.RunSynchronously |> printfn "%A"
   
   
    r.Get "name"B |> Async.RunSynchronously |> printfn "%A" 
//    r.Select 5 |> check "Select 5" "OK"B
//    r.FlushDb () |> check "FlushDb" "OK"B
//    
//    r.Set "k1"B "v1"B |> check "Set k1 to v1" "OK"B
//    
//    r.Get "k1"B |> check "Get k1" "v1"B
    
    Console.ReadLine () |> ignore 
    0

