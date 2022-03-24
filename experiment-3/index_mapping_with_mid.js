curl -H 'Content-Type: application/json' -XGET "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/camp_doc_10k_sku/_mappings?pretty"
curl --header "Content-Type: application/json" -XGET "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/_cat/indices?pretty"

curl -H 'Content-Type: application/json' -XGET "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/camp_doc_10k_sku/_mappings?pretty"

curl -H 'Content-Type: application/json' -XDELETE "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/camp_doc_10k_sku"

curl -H 'Content-Type: application/json' -XPUT "https://vpc-es-benchmarking-test-tg4mvjtk2uzeba4wvby3hanfy4.us-east-1.es.amazonaws.com/camp_doc_10k_sku" -d '{
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
				},
				"tenant": {
					"type": "keyword"
				},
				"campaign_id": {
					"type": "keyword"
				},
				"c_sku_id": {
					"type": "keyword"
				},
				"c_brand": {
					"type": "keyword"
				},
				"c_tlc": {
					"type": "keyword"
				},
				"c_mlc": {
					"type": "keyword"
				},
				"c_llc": {
					"type": "keyword"
				},
				"c_group": {
					"type": "keyword"
				},
				"r_sku_id": {
					"type": "keyword"
				},
				"r_brand": {
					"type": "keyword"
				},
				"r_tlc": {
					"type": "keyword"
				},
				"r_mlc": {
					"type": "keyword"
				},
				"r_llc": {
					"type": "keyword"
				},
				"r_group": {
					"type": "keyword"
				}
				
			}
		}
	}
}'