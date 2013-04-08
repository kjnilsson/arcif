namespace Arcif

module Connection =

    open System
    open System.IO
    open System.Net.Sockets
    open Arcif
    open Arcif.Protocol

    let toBytes (s:string) =
        System.Text.Encoding.UTF8.GetBytes s

    type AsyncReply = AsyncReplyChannel<Reply>

    type Command =
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
            |> List.map Bulk
            |> Multi

        let keyCommand cmd key s =
            Protocol.multiP (multi [cmd; key]) s 
            Protocol.redisU s

        let agent = MailboxProcessor.Start(fun inbox ->
            let rec loop s = async {
                let! msg = inbox.Receive()
                match msg with
                    | Select (db, rc) -> 
                        let db = db.ToString() |> toBytes
                        return rc.Reply (keyCommand "SELECT"B db s)
                    | Set (key, v, rc) ->
                        Protocol.multiP (multi ["SET"B; key; v]) s 
                        return rc.Reply (Protocol.redisU s)
                    | Get (key, rc) ->
                        return rc.Reply (keyCommand "GET"B key s)
                    | MGet (keys, rc) ->
                        Protocol.multiP (multi ("MGET"B :: keys)) s 
                        return rc.Reply (Protocol.redisU s)
                    | LPush (key, vals, rc) ->
                        Protocol.multiP (multi ("LPUSH"B :: key :: vals)) s 
                        return rc.Reply (Protocol.redisU s)
                    | LPop (key, rc) ->
                        return rc.Reply (keyCommand "LPOP"B key s)       
                    | RPopLPush (keyfrom, keyto, rc) ->
                        Protocol.multiP (multi ["RPOPLPUSH"B; keyfrom; keyto]) s 
                        return rc.Reply (Protocol.redisU s)         
                    | LLen (key, rc) ->
                        return rc.Reply (keyCommand "LLEN"B key s)
                    | Eval (script, keys, args, rc) ->
                        let numKeys = keys.Length.ToString() |> toBytes
                        Protocol.multiP (multi ("EVAL"B :: script :: numKeys :: (List.concat [keys; args]))) s 
                        return rc.Reply (Protocol.redisU s)
                    | MultiBlock rc -> 
                        Protocol.multiP (multi ["MULTI"B]) s
                        return rc.Reply (Protocol.redisU s)
                    | Exec rc -> 
                        Protocol.multiP (multi ["EXEC"B]) s
                        return rc.Reply (Protocol.redisU s)
                    | FlushDb rc -> 
                        Protocol.multiP (multi ["FLUSHDB"B]) s
                        return rc.Reply (Protocol.redisU s)
                    | _ -> failwith "unimplemented redis msg type"
                return! loop s    }
            loop (connect()))

        member this.Select db = agent.PostAndAsyncReply(fun rc -> Select(db, rc))
        member this.Set k v = agent.PostAndAsyncReply(fun rc -> Set(k, v, rc))
        member this.Get k = agent.PostAndAsyncReply(fun rc -> Get(k, rc))
        member this.MGet keys = agent.PostAndAsyncReply(fun rc -> MGet(keys, rc))
        member this.LPush key vals = agent.PostAndAsyncReply(fun rc -> LPush(key, vals, rc))
        member this.LPop key = agent.PostAndAsyncReply(fun rc -> LPop(key, rc))
        member this.LLen key = agent.PostAndAsyncReply(fun rc -> LLen(key, rc))
        member this.Eval script keys args = agent.PostAndAsyncReply(fun rc -> Eval(script |> toBytes, keys, args, rc))
        member this.RPopLPush keyfrom keyto = agent.PostAndAsyncReply(fun rc -> RPopLPush(keyfrom, keyto, rc))
        member this.Multi () = agent.PostAndAsyncReply(fun rc -> MultiBlock rc)
        member this.Exec () = agent.PostAndAsyncReply(fun rc -> Exec rc)
        member this.FlushDb () = agent.PostAndAsyncReply(fun rc -> FlushDb rc)


        interface IDisposable with
            member this.Dispose() =
                ()
module Redis =
    let connect (ip:string) (port:int32) =
        new Connection.RedisAgent(ip, port)
