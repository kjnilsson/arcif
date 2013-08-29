namespace Arcif.Redis

module Connection =

    open System
    open System.IO
    open System.Net.Sockets
    open Arcif
    open Arcif.Redis

    let toBytes (s:string) =
        System.Text.Encoding.UTF8.GetBytes s

    type Command =
        | Exit
        | Command of (Parser.OutState -> unit) * AsyncReplyChannel<Reply>
        
    type RedisAgent (ip:string, port) =
        let connect() =
            let sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            sock.Connect(ip, port)
            new BufferedStream(new NetworkStream (sock))
        
        let multi (items : byte[] list) =
            items
            |> List.map BulkReq
            |> MultiReq

        let agent = MailboxProcessor.Start(fun inbox ->
            let rec loop s = async {
                let! msg = inbox.Receive()
                match msg with
                | Command (write, rc) ->
                    write s 
                    return rc.Reply (Parser.redisU s)
                | Exit -> s.Dispose()
                | _ -> failwith "unimplemented redis msg type"
                return! loop s    }
            loop (connect()))
            
        member this.PostAndAsyncReply f = agent.PostAndAsyncReply f

        interface IDisposable with
            member this.Dispose() =
                agent.Post Exit
                ()

open Connection

[<AutoOpen>]
module Shared =

    let multi (items : byte[] list) =
        items
        |> List.map BulkReq
        |> MultiReq

    let command write = fun rc -> Command (write, rc)
  
module Server =

    type RedisAgent with
        
        member this.Select (db : int) =
            let db = db.ToString() |> toBytes
            Parser.redisP (multi ["SELECT"B; db])
            |> command 
            |> this.PostAndAsyncReply
        member this.FlushDb () = 
            Parser.redisP (multi ["FLUSHDB"B])
            |> command 
            |> this.PostAndAsyncReply         
        
module Transactions =

    type RedisAgent with
        member this.Multi () = 
            Parser.redisP (multi ["MULTI"B])
            |> command
            |>  this.PostAndAsyncReply
        member this.Exec () = 
            Parser.redisP (multi ["EXEC"B])
            |> command
            |> this.PostAndAsyncReply
           
module Scripting =

    type RedisAgent with
        member this.Eval script (keys : byte [] list) args = 
            let numKeys = keys.Length.ToString() |> toBytes
            Parser.redisP (multi ("EVAL"B :: (script |> toBytes) :: numKeys :: (List.concat [keys; args])))
            |> command
            |> this.PostAndAsyncReply
                      
module Lists =

    type RedisAgent with
        member this.LPush key vals = 
            Parser.redisP (multi ("LPUSH"B :: key :: vals))
            |> command
            |> this.PostAndAsyncReply
        member this.LPop key = 
            Parser.redisP (multi ["LPOP"B; key])
            |> command
            |> this.PostAndAsyncReply
        member this.LLen key = 
            Parser.redisP (multi ["LLEN"B; key])
            |> command
            |> this.PostAndAsyncReply
        member this.RPopLPush keyfrom keyto = 
            Parser.redisP (multi ["RPOPLPUSH"B; keyfrom; keyto])
            |> command
            |> this.PostAndAsyncReply
        
module Strings =

    type RedisAgent with
        member this.Set key value = 
            Parser.redisP (multi ["SET"B; key; value])
            |> command
            |> this.PostAndAsyncReply
        member this.Get key = 
            Parser.redisP (multi ["GET"B; key])
            |> command
            |> this.PostAndAsyncReply
        member this.MGet keys = 
            Parser.redisP (multi ("MGET"B :: keys))
            |> command
            |> this.PostAndAsyncReply

module Redis =
    let connect (ip:string) (port:int32) =
        new Connection.RedisAgent(ip, port)
