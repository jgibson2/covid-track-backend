# Track COVID-19 Cases Through User-Supplied Anonymized Data

Example GET:
`http://142.93.253.235/track?cell_token=87df3528f`

Example POST:
```json
{
	"data" : [
			{"cell_token": "87df3528f", "timestamp": 1586242911000},
      			{"cell_token": "87df3528f", "timestamp": 1586242991000}
		]
}
```

Cell Token is the S2 cell token generated from the library. It is a unique alphanumeric string encoding the ID.
