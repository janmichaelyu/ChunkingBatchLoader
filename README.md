# ChunkingBatchLoader
A batch loader that gathers rows into chunks, both by grouping into records, and by grouping records into chunks for faster insert

Loader reads a .csv file sorted by some column groups records based on that column having the same values E.g column 35 is a PersonID index, so all records for that person are grouped (due to sorting) and the loader will group them to one record.

Most configuration is hard-coded in the Loader class.

Two strategies are implemented:
* XccMLServiceCallChunkWriter.java - sends a larger JSON structure with a batch of records to be processed and inserted as one transaction
* XccMulitItemInsertChunkWriter.java  - uses XCC to send an array of separate JSON structures (sent as Strings)

Basic approach is a single file-reader that reads and chunks up lines from a CSV file, feeding them to a queue, then a set of writer threads that consume data from that queue and write it to MarkLogic. So the File reader is a "producer" to the queue, and the various writers are "consumers" from the queue, and the code implements a classic producer/consumer pattern.
