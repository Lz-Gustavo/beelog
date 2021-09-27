# beelog
*beelog* package implements a series of recovery mechanisms for SMR applications, relying on efficient data structures and log compaction algorithms to safely discard entries from the command log. *beelog* protocol is orthogonal to any checkpoint implementation, providing a simple API for log configuration and recovery.

**IMPORTANT:** This repository contains the log compaction library on its exact version as evaluated on [*Shrinking Logs by Safely Discarding Commands*](https://sol.sbc.org.br/index.php/sbrc/article/view/16749), published at SBRC 2021. The concurrent structure mentioned on the paper is the *ConcTable*, as implemented in ```conctable.go```. A minor version of this package, containing only the conctable structure and test procedures, is available at [Lz-Gustavo/beemport](https://github.com/Lz-Gustavo/beemport/tree/master). On that repo, some improvements will be conducted on the actual structure, but the same version as published on the paper will always be available at the [v0.1-SBRC21 branch](https://github.com/Lz-Gustavo/beemport/tree/v0.1-SBRC21).

## Related repositories
* [Lz-Gustavo/beelog-hraft](https://github.com/Lz-Gustavo/beelog-hraft)

	Implements a minimal key-value store application backed by the hashicorp/raft consensus algorithm, used as a SMR prototype on *beelog* evaluations.

* [Lz-Gustavo/beexecutor](https://github.com/Lz-Gustavo/beexecutor)

	Implements a local key-value store parsing static input logs, measuring *beelog* efiency (throughput, flush latency) against a standard logging scheme.

* [Lz-Gustavo/go-ycsb](https://github.com/Lz-Gustavo/go-ycsb)

	Implements the database interfaces of go-ycsb, a Go port of the popular Yahoo! Cloud Serving Benchmarking tool, for the beelog-hraft key-value store. Also, allows the generation of static input log files to bechmark *beexecutor* with its predefined workloads.
