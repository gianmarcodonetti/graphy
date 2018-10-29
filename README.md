# graphy
Graph computations.

The implemented methods are:
- Vertex Smoothing in python

## Vertex Smoothing

Smoothing out or smoothing a vertex ***w*** with regards to the pair of edges (e1, e2) incident on ***w***, removes both edges containing ***w*** and replaces (e1, e2) with a new edge that connects the other endpoints of the pair.

For example, the simple connected graph with two edges, e1 {u,w} and e2 {w,v}:,

![alt text](https://upload.wikimedia.org/wikipedia/commons/6/6f/Graph_subdivision_step2.svg )

has a vertex (namely w) that can be smoothed away, resulting in:

![alt text](https://upload.wikimedia.org/wikipedia/commons/1/15/Graph_subdivision_step1.svg )
