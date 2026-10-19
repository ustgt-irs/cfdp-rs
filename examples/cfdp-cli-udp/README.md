CFDP CLI Example with UDP servers
=======

This example models two complete CFDP entities as separate applications. Each entity runs a
source handler, a destination handler and a UDP server, so both entities can send and receive
files. It also shows proxy put request handling.

The local entity (ID 1) uses port 5111 and the remote entity (ID 2) uses port 5222.
Transactions are logged as `<source entity ID>:<sequence number>`, for example `1:0` for the
first transaction started by the local entity.

Start the remote entity first:

```sh
cargo run -p cfdp-cli-udp --bin remote
```

Send a file from the local entity to the remote entity:

```sh
mkdir -p files/local files/remote
echo "Hello World!" > files/local/hello.txt
cargo run -p cfdp-cli-udp --bin local -- files/local/hello.txt files/remote/hello.txt
```

Use a proxy put request to instruct the remote entity to send the file back:

```sh
cargo run -p cfdp-cli-udp --bin local -- -p files/remote/hello.txt files/local/hello-sent-back.txt
```

The remote entity sends the file back and then sends a proxy put response to the local entity.
The `-m` and `-c` options set the transmission mode and the closure request. With `-p`, they are
sent as proxy messages and apply to the transfer from the remote entity.

Run `cargo run -p cfdp-cli-udp --bin local -- -h` for all options.
