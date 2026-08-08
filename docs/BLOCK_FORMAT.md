# Experimental block format version 0

Version 0 is little-endian and intentionally has no compatibility promise. Callers choose only a
logical type (`boolean`, `number`, `string`, or `datetime`) and compression. Physical encodings are
owned by the format implementation.

## Header

The fixed header is 36 bytes:

| Offset | Width | Field                                      |
| -----: | ----: | ------------------------------------------ |
|      0 |     4 | ASCII magic `BRDB`                         |
|      4 |     2 | format version (`0`)                       |
|      6 |     2 | header length (`36`)                       |
|      8 |     1 | logical type ID                            |
|      9 |     1 | physical encoding ID                       |
|     10 |     1 | compression ID                             |
|     11 |     1 | mandatory flags; must be zero              |
|     12 |     4 | row count                                  |
|     16 |     4 | null count                                 |
|     20 |     4 | metadata byte length                       |
|     24 |     4 | uncompressed encoded byte length           |
|     28 |     4 | stored payload byte length                 |
|     32 |     4 | CRC-32 of the uncompressed encoded payload |

The header is followed by UTF-8 JSON metadata and then the compressed payload. Decoders validate
all identifiers, counts, lengths, metadata, decompressed size, and checksum before decoding values.
The current implementation caps each metadata or payload section at 64 MiB.

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
validates a block while retaining its version-zero physical payload. `validatePhysicalColumn()`
checks an uncompressed payload directly. `measurePhysicalColumnRanges()` computes the exact output
allocation and canonical metadata for half-open row ranges; `buildPhysicalColumnFromRanges()`,
`slicePhysicalColumn()`, and `concatenatePhysicalColumns()` construct the corresponding bitmaps,
fixed-width values, or string offsets/content. `encodePhysicalBlock()` validates and writes that
payload with raw, RLE, or gzip compression.

These APIs preserve the same canonical null-slot, bitmap-padding, numeric/datetime, UTF-8, offset,
metadata, checksum, and 64 MiB section rules as ordinary row-value encode/decode. They avoid row
objects; they do not by themselves impose an executor memory budget or account for browser-native
codec allocations.
