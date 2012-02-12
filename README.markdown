cloudtools
==========

Install
-------

Install using:

	$ python setup.py

(more docs to come)

Design math
-----------

Objective: 100 billion particles cycling every 5 mins (300s)

Simulation characteristics (est.):

 * Each particle: 1kb (incl. serialization overhead)
 * Soup: 100 billion particles = 93 TB = $9,830 / month

PiCloud characteristics:

Small instances (c1):

 * Local disk write throughput (/tmp): >100 MB/s
 * S3 write throughput (1 thread): ~10 MB/s
 * S3 chunked read throughput (1 thread, 64MB chunks): ~25 MB/s
 * S3 chunked read throughput (1 thread, 128MB chunks): ~21 MB/s

Medium instances, high i/o (m1):

 * S3 write throughput (1 thread): ~17MB/s
 * S3 chunked read throughput (1 thread, 128MB chunks): ~20 MB/s
 
Conservative estimates:

 * 93 TB / 300 s = 317 GB/s
 * Mappers output their inputs, plus indexing (out:in = 1.4, est.)
 * Mappers collectively read 93 TB, output 130 TB
 * 130 TB / 10 MB/s = 13 MILLION mappers!
 * might need to revise design objectives...

Observations
------------

 * Need multi-threading to saturate m1 IO to S3 (S3 has per-connection limits)
 * Large intermediates and slow S3 writes make mapper writes the bottle-neck
   * 'Real' MR has reducers read directly from mapper disks so both storage and
   upstream bandwidth is distributed over the many mappers
   * No incoming connections, so not possible with picloud