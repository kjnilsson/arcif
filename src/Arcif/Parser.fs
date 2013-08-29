namespace Arcif.Redis

module Parser =
    
    open System.IO

    type OutState = System.IO.Stream
    type InState = System.IO.Stream

    type Pickler<'T> = 'T -> OutState -> unit
    type Unpickler<'T> = InState -> 'T

    let CRLF = "\r\n"B
    
    let str x = x.ToString()

    let utf8String = System.Text.Encoding.UTF8.GetString
    
    let private getBytes (s:string)= System.Text.Encoding.UTF8.GetBytes s

    let private byteP (b:byte) (st:OutState) = st.WriteByte b
    let private byteU (st:InState) = byte(st.ReadByte())

    let private statusU (st:InState) = 
        let rec loop acc =
            let b = byte(st.ReadByte())
            match b with 
            | '\r'B -> 
                st.ReadByte() |> ignore //read the \n as well
                utf8String (acc |> Array.ofList |> Array.rev)
            | _ -> loop (b :: acc)
        loop List.empty

    let private errorU = statusU

    let private integerU (st:InState) =
        let s = statusU st
        System.Int64.Parse s


    let readBytes (st:Stream) n = 
        let buffer = Array.create<byte> n 0uy
        st.Read (buffer, 0, n)
        buffer

    let private integerP i (st:OutState) =
        let is = str i |> getBytes
        st.Write (is, 0, is.Length)
        st.Write (CRLF, 0, CRLF.Length)

    let private crlfP (st:OutState) =
        st.Write(CRLF, 0, CRLF.Length)

    let private bulkU (st:InState) =
        let len = integerU st |> int32
        if (len = -1) then
            None
        else
            let result = readBytes st len
            readBytes st 2 |> ignore // CRLF
            Some result

    let private elementP (data:BulkReq) (st:OutState) =
        let (BulkReq d) = data
        let els = str d |> getBytes 
        st.WriteByte ('$'B)
        st.Write (els, 0, els.Length)
        crlfP st
        st.Write (d, 0, d.Length)
        crlfP st

    let private multiU (st:InState) elU =
        let num = integerU st |> int32
        if (num = -1) then 
            None
        else
            [0 .. num - 1]
            |> List.map (fun _ -> elU st)
            |> Some

    let rec private elementU (st:InState) =
        match byteU st with
        | '+'B -> Status (statusU st)
        | '-'B -> Error (errorU st)
        | ':'B -> Integer (integerU st)
        | '$'B -> Bulk (bulkU st)
        | '*'B -> Multi (multiU st elementU)
        | x -> failwith (sprintf "%c is an invalid Redis element type" (char x))

    let private multiP (m:MultiReq) (st:OutState) =
        let (MultiReq (bulkList)) = m
        let count = bulkList.Length |> int64
        byteP '*'B st
        integerP count st
        for bulk in bulkList do
            elementP bulk st
            
    let redisP = multiP

    let redisU (st:InState) =
        elementU st
