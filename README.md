# beelog
Simulates different log compactation algorithms utilizing random load profiles or statically defined logs. Allows the implementation of new representation structures and algorithms following simple interfaces.

## Usage
1. Test cases are configured by a simple .TOML file.
```toml
Name="An1"
Struct=1
NumCmds=100
PercentWrites=50
NumDiffKeys=10
Iterations=3
Algo=[2, 3]
```

2. If a ```LogFile``` parameter is provided, ```NumCmds```, ```PercentWrites```, and ```NumDiffKeys``` are ignored and the static log is parsed from the provided path.

3. Experimental metrics are written on **stdout**, and the compacted log of commands in dumped on a **.out** file.

4. A comparison between algorithms and additional benchmarks are available at **reduce_test.go**.