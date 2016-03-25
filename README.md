# Customized Collaborative Recommender Template
##Customization Description
The original template from PredictionIO used here was the [similar product recommender](https://templates.prediction.io/PredictionIO/template-scala-parallel-similarproduct). From this, several changes have been made.

####Method of generating recommendations
The original template accepts an item as input and finds the other items with the most similar rating vectors in the predicted matrix. This customized version calculates trains the matrix in the same way but takes a user as input and returns the items with the highest predicted rating. This is how recommendations in the [basic recommendation template](https://templates.prediction.io/PredictionIO/template-scala-parallel-recommendation) are made.

####Item Properties
This template adds item properties. An item now has a title and category. [PredictionIO example code and guide](https://github.com/PredictionIO/PredictionIO/blob/develop/examples/scala-parallel-similarproduct/add-and-return-item-properties/README.md) was followed to make this change to the code

####No need to set users
In the original template, a user needed to be "set" before sending events for that user. This customization does not have that, only items need to be declared before sending events that use them. [PredictionIO example code and guide](https://github.com/PredictionIO/PredictionIO/tree/develop/examples/scala-parallel-similarproduct/no-set-user) was used to make this change.

####Already-viewed items blacklist
Users will only be recommended items which they have not previously viewed (according to the view events in the event database). [A PredictionIO guide](https://docs.prediction.io/templates/recommendation/blacklist-items/) and code from the [ecommerce template](https://github.com/PredictionIO/template-scala-parallel-ecommercerecommendation) was used as example.

## Versions
### v0.3.3

- Version used to apply customizations from this fork

### v0.3.2

- Fix CooccurrenceAlgorithm with unknown item ids

### v0.3.1

- Add CooccurrenceAlgorithm.
  To use this algorithm, override `engine.json` by `engine-cooccurrence.json`,
  or specify `--variant engine-cooccurrence.json` parameter for both `$pio train` **and**
  `$pio deploy`

### v0.3.0

- update for PredictionIO 0.9.2, including:

  - use new PEventStore API
  - use appName in DataSource parameter


### v0.2.0

- update build.sbt and template.json for PredictionIO 0.9.2

### v0.1.3

- cache mllibRatings RDD in algorithm train() because it is used at multiple places (non-empty data check and ALS)

### v0.1.2

- update for PredictionIO 0.9.0

### v0.1.1

- Persist RDD to memory (.cache()) in DataSource for better performance
- Use local model for faster serving.

### v0.1.0

- initial version
