config = {"debug": True,
      "cache": "Cache",
      "redis": {"host": "redis",
		"port": 6379,
		"ttl": 604800},
      "rest_api": {"host": "localhost",
		   "port": 18150},
      # Blazegraph SPARQL Endpoint
      "triplestore": {"host": "semantic_server",
		      "port": 8080,
		      "path": "bigdata"}
}
