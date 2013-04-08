namespace Arcif.Tests
open System
open System.IO
open NUnit.Framework
open Arcif
open Protocol

module ProtocolTests = 
    let testProtocol() =

        let testStatus = "+OK\r\n"B
        let testError = "-ERR error\r\n"B
        let testInteger = ":1234\r\n"B
        let testBulk = "$6\r\nfoobar\r\n"B
        let testBulkNull = "$-1\r\n"B
        let testMulti = "*3\r\n:123\r\n$-1\r\$5\r\nnHello\r\n"B
        let testMultiEmpty = "*0\r\n"B
        let testMultiNull = "*-1\r\n"B

        let testf (reply:byte[]) f =
            use stream = new MemoryStream(reply)
        
            f stream
            |> printfn "%A"

            stream.Close()

        let test (reply:byte[]) =
            testf reply redisU


        test testStatus
        test testError
        test testInteger
        test testBulk
        test testBulkNull
        test testMulti
        test testMultiEmpty
        test testMultiNull

        testf "-1\r\n"B integerU 

        let test2 m =
            use ms = new MemoryStream()
            multiP m ms
            ms.Position <- 0L
            use sr = new StreamReader(ms)
            sr.ReadToEnd() |> printfn "%A"

        let request = Multi [Bulk "SET"B; Bulk "Key"B; Bulk "Value"B]
        test2 request
        
    [<Test>]
    let ``test protocol`` () =
        testProtocol()
