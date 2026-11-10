// Package compression provides the Snappy codec and pooled batch workers.
// Workers support uncompressed fallback when compression does not shrink a
// payload. The transfer runtime bypasses codecs entirely for none mode.
package compression
