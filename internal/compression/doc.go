// Package compression provides uncompressed and Snappy codecs for pipeline
// payloads. Codecs write into caller-provided destination buffers and report
// insufficient capacity or invalid compressed data as errors.
package compression
