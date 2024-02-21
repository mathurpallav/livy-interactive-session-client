# livy-interactive-session-client
Creating a Python Client for Livy Interactive session

Apache Livy provides an Interactive Session which is a great tool for running a spark-shell on any spark cluster and remotely submit statements/spark query over the cluster from any machine and programming language. It allows apps of different environments or programming language or leverage computing power of Spark 

Read more about it at my medium article - Link

This client is just an example of how one can write a client to use Livy Interactive Session from their code/app

Livy Interactive Session API Docs - https://livy.apache.org/docs/latest/rest-api.html

APIs Used - 
- Post Session
- Get Session
- Post Statement
- Get Statement
- Delete Session
