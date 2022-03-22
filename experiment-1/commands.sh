curl --header "Content-Type: application/json" -XPUT https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/test_segments -d '
{
    "mappings": {
        "_doc": {
            "properties": {
                "bbstar": {
                    "type": "keyword"
                },
                "cp": {
                    "type": "keyword"
                },
                "dc": {
                    "type": "keyword"
                },
                "ds": {
                    "type": "keyword"
                },
                "emails": {
                    "type": "keyword"
                },
                "entry_context": {
                    "type": "keyword"
                },
                "mtype": {
                    "type": "keyword"
                },
                "phone_numbers": {
                    "type": "keyword"
                },
                "sa_city_ids": {
                    "type": "keyword"
                },
                "sa_ids": {
                    "type": "keyword"
                },
                "source": {
                    "type": "keyword"
                },
                "status": {
                    "type": "keyword"
                }
            }
        }
    },
    "settings": {
        "search": {
            "slowlog": {
                "threshold": {
                    "query": {
                        "debug": "300ms"
                    }
                }
            }
        },
        "number_of_shards": "1",
        "number_of_replicas": "1"
    }
}'

curl --header "Content-Type: application/json" -XPUT https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/test_segments_with_mid -d '
{
    "mappings": {
        "_doc": {
            "properties": {
                "bbstar": {
                    "type": "keyword"
                },
                "cp": {
                    "type": "keyword"
                },
                "dc": {
                    "type": "keyword"
                },
                "ds": {
                    "type": "keyword"
                },
                "emails": {
                    "type": "keyword"
                },
                "entry_context": {
                    "type": "keyword"
                },
                "mid": {
                    "type": "keyword"
                },
                "mtype": {
                    "type": "keyword"
                },
                "phone_numbers": {
                    "type": "keyword"
                },
                "sa_city_ids": {
                    "type": "keyword"
                },
                "sa_ids": {
                    "type": "keyword"
                },
                "source": {
                    "type": "keyword"
                },
                "status": {
                    "type": "keyword"
                }
            }
        }
    },
    "settings": {
        "search": {
            "slowlog": {
                "threshold": {
                    "query": {
                        "debug": "300ms"
                    }
                }
            }
        },
        "number_of_shards": "1",
        "number_of_replicas": "1"
    }
}'




curl --header "Content-Type: application/json" -XDELETE https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/test_segments



curl --header "Content-Type: application/json" -XGET https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/test_segments/_count

curl --header "Content-Type: application/json" -XGET https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/test_segments/_mapping