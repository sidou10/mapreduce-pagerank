# PageRank with MapReduce
- Initial input: list of edges (FromNodeId -> ToNodeId)
```
0	4
0	5
0	7
0	8
0	9
0	10
...
```
- Final output: list of (Pagerank, NodeId) sorted in ascending order
The end of the final output contains the nodes with highest pagerank score
```
...
0.0010894685313624648	725
0.0011258835352380885	1619
0.0013345636473353036	40
0.0014273088492398204	143
0.0014402293713838919	790
0.0014530810388189337	136
0.001521890178672624	1719
0.0015522353563948235	118
0.002301745029639555	737
0.0033065766170423476	18
```

## Pipeline:
1. Preprocessing MapReduce to get a list of (nodeId, initialPageRank / [adjacencyList])
Example: 
```
75885	1.3178876896110913E-5 / [7900, 16086]
```
2. Iteration MapReduce to repete until convergence (10 rounds for our particular example)
Example after 5 iterations:
```
75885	1.9768315344166374E-6 / [7900, 16086]
```
3. Postprocessing to sort pageranks (see final output)
