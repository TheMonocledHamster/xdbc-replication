# XDBC-Go Compression Stage Implementation V1

## Compression Algorithm Implementations

- Snappy
- None (Uncompressed)

## Functionality Provided by a Compression Codec

- ID()
- Decompress()
- Compress()
- MaxEncodedLen()

*Note: For None codec, (de-)compress is just a copy and MaxEncodedLen is effectively a no-op returning the input*

## Additional Functionality part of the Compression package

- ByID()
- RequiredPayloadCap()