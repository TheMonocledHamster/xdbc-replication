// Package pool manages reusable, fixed-size buffers in a contiguous memory slab
// and encodes their headers. Callers acquire buffers by ID and own their bytes
// until they release the ID or transfer ownership to another pipeline stage.
package pool
