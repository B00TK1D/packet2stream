# packet2stream

Read in packets from a MongoDB collection and tag them with the stream they belong to.

## Usage
```
$ ./packet2stream --help
  -c string
        Mongodb collection name for packets (default "packets")
  -d string
        Mongodb database name (default "traffic")
  -m string
        Mongodb URI (default "mongodb://localhost:27017")
  -s string
        Mongodb collection name for streams (default "streams")
  -t int
        Stream timeout in seconds (default 60)
```