# graphy

[![Build Status](https://travis-ci.org/gianmarcodonetti/graphy.svg?branch=master)](https://travis-ci.org/gianmarcodonetti/graphy)
[![Test Coverage](https://codecov.io/gh/gianmarcodonetti/graphy/branch/master/graph/badge.svg)](https://codecov.io/gh/gianmarcodonetti/graphy)

Graph computations.

The implemented methods are:
- Vertex Smoothing in python

## Vertex Smoothing

Smoothing out or smoothing a vertex ***w*** with regards to the pair of edges (e1, e2) incident on ***w***, removes both edges containing ***w*** and replaces (e1, e2) with a new edge that connects the other endpoints of the pair.

For example, the simple connected graph with two edges, e1 {u,w} and e2 {w,v}:

![alt text](https://upload.wikimedia.org/wikipedia/commons/6/6f/Graph_subdivision_step2.svg )

has a vertex (namely w) that can be smoothed away, resulting in:

![alt text](https://upload.wikimedia.org/wikipedia/commons/1/15/Graph_subdivision_step1.svg )

The service **vertex_smoothing** requires mainly:
1. *links*, a collection of record, representing the links in the considered graph, could be in the form of a list, a Spark RDD or a Spark DataFrame;
2. *vertices_to_remove*, a collection of named nodes to smooth out.

Each record inside the *links* collection should contain the specification of a single link, that is:
1. the source information;
2. the target information;
3. the link between the source and the target information.

Each point above could be represented as the user wants. Indeed, the user has also to provide the way to retrieve all these information through the *source_getter*, *target_getter*, *link_getter* parameters.
