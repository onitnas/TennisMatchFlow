# Tennis Match Flow

### Description

Tennis Match Flow is a real-time tennis match analysis and visualization project. The system collects data from multiple sources, enriches it through microservices, processes it, and displays it on an interactive dashboard. The goal is to provide up-to-date statistics and insights on matches, with potential applications in betting, scouting, and technical analysis.

<hr>

### System Architecture

The project is structured as a microservices architecture:

<ol>
<li>Data Ingestion
<ul>
<li>Real-time data collection using Kafka Producer.
<li>Data sources: Live tennis match feeds from Tennis API.
</ul>
<li>Messaging and Streaming
<ul>
<li>All data is sent through Kafka (Connect and Stream modes) to ensure reliable and scalable streaming.
</ul>
<li>Data Processing
<ul>
<li>Spark Structured Streaming processes data in real-time.
<li>Data enrichment is performed through microservices and machine learning (Spark MLlib, Spark NLP).
</ul>
<li>Storage
<ul>
<li>Processed data is stored in Elasticsearch/OpenSearch with Open Table Format support for advanced querying and versioning.
</ul>
<li>Visualization
<ul>
<li>Interactive dashboards built with Kibana.
<li>Support for maps, charts, and dynamic filters.
</ul>
</ol>
<hr>

### Technologies Used

<li><b>Data Ingestion:</b> Apache Kafka Producer

<li><b>Message Broker:</b> Apache Kafka (Connect, Stream, KRaft)

<li><b>Data Processing:</b> Apache Spark

<li><b>Storage:</b> Elasticsearch/OpenSearch

<li><b>Dashboard:</b> Kibana

<li><b>Languages:</b> Python
