# Gremlinifier

The backend API connecting with Cosmos DB (Gremlin), to power the [graph database visulization](https://github.com/xdqc/kankerpop-visualizer) of linked data. 

The customizable scripts can also ingest free texts and semi-structued data into a graph network.

## Use cases

### Oncology network

[![cancer-logic-model](/docs/cancerpop-visualizer.png)](https://github.com/xdqc/kankerpop-visualizer)
(pseudonymized linked data of oncology study from different sources)

From multidimensional relational databases and freetext reports to a semantic network of clinics, diagnosis (pathological, radiological, genomics), deseases (tumors, metastasis), treatments (surgery, radiotherapy, medication), responses and assessments data.

### History events graph

![history-events-example](/docs/history-events-example.png)
(a glimps of the middle east around 2700 years ago)

Wikipedia articles are recording events for each calendar year. The intention is to generate a knowledge graph of historical events of the entier human history... or *only for the parts properly recorded in Wikipedia :-)* 

The HTML pages are relatively structured starting from 700 BC, For example https://en.wikipedia.org/wiki/700_BC. 

A web scrapper is built to extract data from these pages, and store information as nodes/edges in the graph database.
