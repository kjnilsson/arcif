
namespace Arcif.Redis

[<AutoOpen>]
module Protocol =
    type Reply =
        | Status of string
        | Error of string
        | Integer of int64
        | Bulk of byte[] option
        | Multi of Reply list option

    type BulkReq = BulkReq of byte[]

    type MultiReq = MultiReq of BulkReq list