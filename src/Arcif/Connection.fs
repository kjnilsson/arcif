namespace Arcif.Redis

module Connection =

    open System
    open System.IO
    open System.Net.Sockets
    open Arcif
    open Arcif.Redis

    let toBytes (s:string) =
        System.Text.Encoding.UTF8.GetBytes s

    type AsyncReply = AsyncReplyChannel<Reply>

    type Command =
        | Exit
        | Select of int * AsyncReply
        | Set of byte[] * byte[] * AsyncReply
        | Get of byte[] * AsyncReply
        | MGet of byte[] list * AsyncReply
        | LPush of byte[] * byte[] list * AsyncReply
        | LPop of byte[] * AsyncReply
        | RPopLPush of byte[] * byte[] * AsyncReply
        | LLen of byte[] * AsyncReply
        | Eval of byte[] * byte[] list * byte[] list * AsyncReply
        | MultiBlock of AsyncReply
        | Exec of AsyncReply
        | FlushDb of AsyncReply
        
        
        
    type RedisAgent (ip:string, port) =
        let connect() =
            let sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            sock.Connect(ip, port)
            new BufferedStream(new NetworkStream (sock))
        
        let multi (items : byte[] list) =
            items
            |> List.map BulkReq
            |> MultiReq

        let keyCommand cmd key s =
            Parser.redisP (multi [cmd; key]) s 
            Parser.redisU s

        let agent = MailboxProcessor.Start(fun inbox ->
            let rec loop s = async {
                let! msg = inbox.Receive()
                match msg with
                    | Select (db, rc) -> 
                        let db = db.ToString() |> toBytes
                        return rc.Reply (keyCommand "SELECT"B db s)
                    | Set (key, v, rc) ->
                        Parser.redisP (multi ["SET"B; key; v]) s 
                        return rc.Reply (Parser.redisU s)
                    | Get (key, rc) ->
                        return rc.Reply (keyCommand "GET"B key s)
                    | MGet (keys, rc) ->
                        Parser.redisP (multi ("MGET"B :: keys)) s 
                        return rc.Reply (Parser.redisU s)
                    | LPush (key, vals, rc) ->
                        Parser.redisP (multi ("LPUSH"B :: key :: vals)) s 
                        return rc.Reply (Parser.redisU s)
                    | LPop (key, rc) ->
                        return rc.Reply (keyCommand "LPOP"B key s)       
                    | RPopLPush (keyfrom, keyto, rc) ->
                        Parser.redisP (multi ["RPOPLPUSH"B; keyfrom; keyto]) s 
                        return rc.Reply (Parser.redisU s)         
                    | LLen (key, rc) ->
                        return rc.Reply (keyCommand "LLEN"B key s)
                    | Eval (script, keys, args, rc) ->
                        let numKeys = keys.Length.ToString() |> toBytes
                        Parser.redisP (multi ("EVAL"B :: script :: numKeys :: (List.concat [keys; args]))) s 
                        return rc.Reply (Parser.redisU s)
                    | MultiBlock rc -> 
                        Parser.redisP (multi ["MULTI"B]) s
                        return rc.Reply (Parser.redisU s)
                    | Exec rc -> 
                        Parser.redisP (multi ["EXEC"B]) s
                        return rc.Reply (Parser.redisU s)
                    | FlushDb rc -> 
                        Parser.redisP (multi ["FLUSHDB"B]) s
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
  
module Server =
    open Connection
    type RedisAgent with
        member this.FlushDb () = this.PostAndAsyncReply(fun rc -> FlushDb rc)          
        
module Transactions =
    open Connection
    type RedisAgent with
        member this.Multi () = this.PostAndAsyncReply(fun rc -> MultiBlock rc)
        member this.Exec () = this.PostAndAsyncReply(fun rc -> Exec rc)
                
module Scripting =
    open Connection
    type RedisAgent with
        member this.Eval script keys args = this.PostAndAsyncReply(fun rc -> Eval(script |> toBytes, keys, args, rc))
                      
module Lists =
    open Connection
    type RedisAgent with
        member this.LPush key vals = this.PostAndAsyncReply(fun rc -> LPush(key, vals, rc))
        member this.LPop key = this.PostAndAsyncReply(fun rc -> LPop(key, rc))
        member this.LLen key = this.PostAndAsyncReply(fun rc -> LLen(key, rc))
        member this.RPopLPush keyfrom keyto = this.PostAndAsyncReply(fun rc -> RPopLPush(keyfrom, keyto, rc))
        
        
module Strings =
    open Connection
    type RedisAgent with
        member this.Select db = this.PostAndAsyncReply(fun rc -> Select(db, rc))
        member this.Set k v = this.PostAndAsyncReply(fun rc -> Set(k, v, rc))
        member this.Get k = this.PostAndAsyncReply(fun rc -> Get(k, rc))
        member this.MGet keys = this.PostAndAsyncReply(fun rc -> MGet(keys, rc))
    
module Redis =
    let connect (ip:string) (port:int32) =
        new Connection.RedisAgent(ip, port)
