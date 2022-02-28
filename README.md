# Gremlinifier

To build graph database (Gremlin API) from free texts and semi-structued data.

## Use cases

### History events graph

![history-events-example](/docs/history-events-example.png)
(a glimps of the middle east around 2700 years ago)

Wikipedia articles are recording events for each calendar year. The intention is to generate a knowledge graph of historical events of the entier human history... or *only for the parts properly recorded in Wikipedia :-)* 

The HTML pages are relatively structured starting from 700 BC, For example https://en.wikipedia.org/wiki/700_BC. 

A web scrapper is built to extract data from these pages, and store information as nodes/edges in the graph database.

### Oncology network

![cancer-logic-model](/docs/cancer-logic-model.jpeg)
(illustration of the oversimplified snowflake schema of HL7 FHIR model)

From multidimensional relational databases and freetext reports to a semantic network of clinics, diagnosis (pathological, radiological, genomics), deseases (tumors, metastasis), treatments (surgery, radiotherapy, medication), responses and assessments data.
