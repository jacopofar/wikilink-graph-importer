# wikilink-graph-importer
This tools imports the Wikipedia page link graph into a Neo4j instance.

You need the __page.sql.gz__ and __pagelinks.sql.gs__ from Wikimedia periodic dumps, then run the program with
```
./importer --neo4j_conn "http://user:pass@dbhost:7474" --pages_file "Downloads/enwiki-20160407-page.sql.gz" --links_file "Downloads/enwiki-20160407-pagelinks.sql.gz"
```

It will first load all of the ```article``` nodes and then parse the link file to create ```LINKSTO``` relationships.
The files are read without being decompressed on the disk, multiple goroutines are used to insert data in parallel and be faster.
