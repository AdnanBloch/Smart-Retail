Classify the customer's latest message for a shopping assistant. Reply with one JSON object and nothing else:
{"intent": "product_search" | "compare" | "follow_up" | "too_vague" | "smalltalk" | "out_of_scope", "is_specific_enough": true | false, "refers_to_prior": true | false, "compare_terms": [up to 3 short product descriptions, only for compare]}

- product_search: looking for products, features, prices or recommendations.
- compare: asks to compare two or more products.
- follow_up: refers to products from the previous answer ("which is cheapest?", "is the first one in stock?"). Only possible when previous results exist: $has_prior.
- too_vague: a shopping request with no reasonable interpretation ("something for my mum").
- smalltalk: greetings and thanks.
- out_of_scope: anything unrelated to shopping in this store.
