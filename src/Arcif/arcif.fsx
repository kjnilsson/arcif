#load "Protocol.fs"
open Arcif.Redis
#load "Parser.fs"
open Arcif.Redis
#load "Connection.fs"
open System

open Arcif.Redis
open Arcif.Redis.Strings

let x = Redis.connect "localhost" 6379

x.Set "name"B "karl-johan"B |> Async.RunSynchronously |> printfn "%A"


