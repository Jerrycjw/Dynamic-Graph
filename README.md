# Dynamic-Graph
This project is trying to build up a dynamic graph on SparkStreaming platform.
It get the real-time input from Twitter and use User's ID and get who this user retweet, and also get these users' ID, when they retweet each other this graph will build an edge between them. And I set a time bound, when every node or edges out of this time bound which is counted when it show up to this graph, these nodes or edges will be deleted.

Acknowledge to the OpenSource Library DynamicGraph, which reference to this API to help me build up dynamic graph, and I have done some modificatio on its source code to make it competible with Spark Platform.
