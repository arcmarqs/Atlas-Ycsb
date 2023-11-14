@0xc0cfd72dc12984c0;

using Rust = import "rust.capnp";
$Rust.parentModule("serialize");

struct Request {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    action      @2 :Data;
}

struct Reply {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    data        @2 :Data;
}