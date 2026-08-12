# Experimental block format version 1

Version 1 is little-endian and intentionally has no compatibility promise. Callers choose only a
logical type (`boolean`, `number`, `string`, or `datetime`) and compression. Physical encodings are
owned by the format implementation.

## Header

The fixed header is 40 bytes:

| Offset | Width | Field                                                                  |
| -----: | ----: | ---------------------------------------------------------------------- |
|      0 |     4 | ASCII magic `BRDB`                                                     |
|      4 |     4 | envelope CRC-32 over bytes `[8, header length + metadata byte length)` |
|      8 |     2 | format version (`1`)                                                   |
|     10 |     2 | header length (`40`)                                                   |
|     12 |     1 | logical type ID                                                        |
|     13 |     1 | physical encoding ID                                                   |
|     14 |     1 | compression ID                                                         |
|     15 |     1 | mandatory flags; must be zero                                          |
|     16 |     4 | row count                                                              |
|     20 |     4 | null count                                                             |
|     24 |     4 | metadata byte length                                                   |
|     28 |     4 | uncompressed encoded byte length                                       |
|     32 |     4 | stored payload byte length                                             |
|     36 |     4 | CRC-32 of the uncompressed encoded payload                             |

The header is followed by UTF-8 JSON metadata and then the compressed payload. The envelope
checksum authenticates every header field after itself plus the metadata JSON, so header-only
readers (zone-map pruning, block inventories) can trust row counts and derived statistics without
decompressing the payload. Decoders verify the envelope before trusting any field, then validate
all identifiers, counts, lengths, metadata, decompressed size, and the payload checksum before
decoding values. The current implementation caps each metadata or payload section at 64 MiB.

## Logical encodings

All columns begin with a validity bitmap where a set bit means non-null. Booleans use a second value
bitmap. Numbers use little-endian IEEE-754 float64 values. Datetimes use float64 Unix milliseconds
and decode to JavaScript `Date` values. Strings use `(rowCount + 1)` little-endian uint32 offsets
followed by UTF-8 bytes.

The physical IDs allow later versions to encode whole-valued numbers more compactly without asking
users to choose between integer widths or changing the logical `number` type.

## Physical column operations

The block-format package exposes physical operations for storage rewrites that must avoid
JavaScript row-object materialization. `decodePhysicalBlock()` decompresses, checksum-verifies, and
validates a block while retaining its physical payload. `validatePhysicalColumn()`
checks an uncompressed payload directly. `measurePhysicalColumnRanges()` computes the exact output
allocation and canonical metadata for half-open row ranges; `buildPhysicalColumnFromRanges()`,
`slicePhysicalColumn()`, and `concatenatePhysicalColumns()` construct the corresponding bitmaps,
fixed-width values, or string offsets/content. `encodePhysicalBlock()` validates and writes that
payload with raw, RLE, or gzip compression.

These APIs preserve the same canonical null-slot, bitmap-padding, numeric/datetime, UTF-8, offset,
metadata, checksum, and 64 MiB section rules as ordinary row-value encode/decode. They avoid row
objects; they do not by themselves impose an executor memory budget or account for browser-native
codec allocations.
