namespace Arcif

module Protocol =

    open System.IO

    type OutState = System.IO.Stream
    type InState = System.IO.Stream

    type Pickler<'T> = 'T -> OutState -> unit
    type Unpickler<'T> = InState -> 'T

    type Reply =
        | Status of string
        | Error of string
        | Integer of int64
        | Bulk of byte[] option
        | Multi of Reply list option

    type Bulk = Bulk of byte[]

    type Multi = Multi of Bulk list

    let CRLF = "\r\n"B

    let utf8String = System.Text.Encoding.UTF8.GetString

    let byteP (b:byte) (st:OutState) = st.WriteByte b
    let byteU (st:InState) = byte(st.ReadByte())

    let statusU (st:InState) = 
        let rec loop acc =
            let b = byte(st.ReadByte())
            match b with 
            | '\r'B -> 
                st.ReadByte() |> ignore //read the \n as well
                utf8String (acc |> Array.ofList |> Array.rev)
            | _ -> loop (b :: acc)
        loop List.empty

    let errorU = statusU

    let integerU (st:InState) =
        let s = statusU st
        System.Int64.Parse s

    let getBytes (s:string)= System.Text.Encoding.UTF8.GetBytes s

    let readBytes (st:Stream) n = 
        async {
            return! st.AsyncRead n
        } |> Async.RunSynchronously

    let integerP i (st:OutState) =
        let is = i.ToString() |> getBytes
        st.Write(is, 0, is.Length)
        st.Write(CRLF, 0, CRLF.Length)

    let crlfP (st:OutState) =
        st.Write(CRLF, 0, CRLF.Length)

    let bulkU (st:InState) =
        let len = integerU st |> int32
        if (len = -1) then
            None
        else
            let result = readBytes st len
            readBytes st 2 |> ignore // CRLF
            Some result

    let elementP (data:Bulk) (st:OutState) =
        match data with
        | Bulk d -> 
            let els = d.Length.ToString() |> getBytes 
            st.WriteByte ('$'B)
            st.Write (els, 0, els.Length)
            crlfP st
            st.Write (d, 0, d.Length)
            crlfP st

    let multiU (st:InState) elU =
        let num = integerU st |> int32
        if (num = -1) then None
        else
            let m = [0 .. num-1] |>
                    List.map (fun _ -> elU st)
            Some m

    let rec elementU (st:InState) =
        match byteU st with
        | '+'B -> Status (statusU st)
        | '-'B -> Error (errorU st)
        | ':'B -> Integer (integerU st)
        | '$'B -> Reply.Bulk (bulkU st)
        | '*'B -> Reply.Multi (multiU st elementU)
        | x -> failwith (sprintf "%c is an invalid Redis element type" (char x))

    let multiP (m:Multi) (st:OutState) =
        match m with
        | Multi bulkList -> 
            let count = bulkList.Length |> int64
            byteP '*'B st
            integerP count st
            for bulk in bulkList do
                elementP bulk st
            
    let redisP = multiP

    let redisU (st:InState) =
        elementU st



