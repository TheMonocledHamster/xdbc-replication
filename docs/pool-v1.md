# XDBC-Go Pool Implementation V1

A config is defined as follows:

```golang
type Config struct {
	NumBuffers int // number of logical buffers in the slab
	PayloadCap int // maximum data in each buffer in bytes
}
```

A pool is defined as follows:

```golang
type Pool struct {
	cfg     Config        // Pool configuration
	slab    []byte        // A raw shared memory segment
	bufSize int           // Defines the size of an entire logical buffer
	free    chan int      // A list of free buffers set up in FIFO order
	used    []atomic.Bool // Optional, but might be nice to have for accounting
}
```

Initialize a Pool with `New(cfg Config)`

## Functionality Provided by a Pool

- `Acquire(ctx context.Context)` yields an available buffer ID
- `Release(ctx context.Context, id int)` returns a buffer to the pool for reuse
- `Buffer(id int) ([]byte, error)` validates the ID and returns the slab-backed
  header and entire payload for a buffer the caller owns
- `checkID(id int)` confirms if an ID is valid for the pool

*Note: For None codec, (de-)compress is just a copy and MaxEncodedLen is effectively a no-op returning the input*

## Additional Functionality part of the Compression package

- ByID()
- RequiredPayloadCap()
