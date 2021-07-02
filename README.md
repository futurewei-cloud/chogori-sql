# Project description

The Chogori-SQL project is a horizontally scalable SQL service with disaggregated storage provided by the Chogori-Platform(https://github.com/futurewei-cloud/chogori-platform) SKV storage service with focus on performance provided by in-memory data handling and fast, kernel bypass network processing. Our main achievement is that we were able to take the industry-standard SQL engine PostgreSQL and rewrite its storage layer to use our novel K23SI transaction protocol with our rich, schema-aware data model - SKV

# Key features
- Horizontally scalable multi-reader/multi-writer architecture
- Industry-standard SQL support thanks to the PostgreSQL engine
- Much improved consistency (Serializable Snapshot Isolation with External Causal Consistency to be more precise) is used in all transactions, thanks to the consistency guarantees provided by the Chogori-Platform SKV service
- Full TPC-C compatibility for all transaction types with 1806.3 transactions per minute / per core(tpmc).
- Performance more than 2x better than the nearest competitor (YugabyteDB) on the TPC-C suite
- Initial Near-Data-Processing effort with computation push-down from SQL to SKV layer, including predicate filtering, projection, and scan prefix bounds.

# Credits
Our work was made possible thanks to YugabyteDB's open source initiative Home | YugabyteDB . This project builds upon PostgreSQL and served as a great starting point to instruct us on how to integrate our custom storage engine with PostgreSQL.

# Getting Started
We've provided a dockerfile which specifies all dependencies needed to build and run our code. Please see [docker/README.md](docker/README.md)
