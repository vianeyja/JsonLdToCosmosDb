# Json-ld to Cosmos Db parser
This repository contains a parser that reads a json ld from a path and inserts the nodes and edges into Cosmos Db Graph API.

## Prerequisites
1. An active Azure account. If you don't have one, you can sign up for a free account. 
2. JDK 1.7+
3. Maven
4. Azure Cosmos Db Gremlin API account, database and graph. You can follow this [tutorial](https://docs.microsoft.com/en-us/azure/cosmos-db/create-graph-dotnet).
5. A [flattened json-ld](https://www.w3.org/TR/json-ld11/#flattened-document-form) file.

## Run this parser
1. Open the src/cluster.yaml file and change the endpoint and authorization key to match your previously created parameters of the Cosmos Db Gremlin API account, database and graph.

| Setting | Suggested Value | Description |
| ------- | --------------- | ----------- |
| <<host>> | [***.gremlin.cosmosdb.azure.com] | This is the Gremlin URI value on the Overview page of the Azure portal, in square brackets, with the trailing :443/ removed.  This value can also be retrieved from the Keys tab, using the URI value by removing https://, changing documents to graphs, and removing the trailing :443/. |
| port | 443 | Set the port to 443 |
| username | `/dbs/<<database>>/colls/<<graph>>` | The resource where `<<database>>` is your database name and `<<graph>>` is your collection name. |
| password | Your primary key | This is your primary key, which you can retrieve from the Keys page of the Azure portal, in the Primary Key box. Use the copy button on the left side of the box to copy the value. |

2.Open the file inside  src/main/resources/pom.properties and change the path to point to your json ld location.
```
 file=C:\\yourfilelocation.json
```

## Expected result
After running this sample, you can login into yhe Azure portal, browse into your Cosmosd Db account, open the query explorer and visualize your data.
1. Nodes from json-ld file with their properties
2. Relationships from json-ld files (specified inside @context in json-ld file)
3. A context node with id 'context' contining all the context properties.
