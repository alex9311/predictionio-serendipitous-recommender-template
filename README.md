# Customized Collaborative Recommender Template
The original template from PredictionIO used here was the [similar product recommender](https://templates.prediction.io/PredictionIO/template-scala-parallel-similarproduct). From this, several changes have been made.
##Usage
- Install predictionio, follow their [guide](https://docs.prediction.io/install/) on this
- Use pio command line to get this template
```
pio template get alex9311/custom-collaborative-recommender SimilarProduct
cd SimilarProduct
```
- Add items to event store, use json to load in bulk. Json for items should have the following format
```json
{"event":"$set","entityType":"item","entityId":"1","properties":{"category":"a","title":"title for a1"}}
{"event":"$set","entityType":"item","entityId":"2","properties":{"category":"a","title":"title for a2"}}
{"event":"$set","entityType":"item","entityId":"3","properties":{"category":"b","title":"title for b3"}}
```
```bash
pio import --appid <your_appid> --input ~/items.json
```
- Add views to event store, same basic idea as items. The view events json file should have the following format
```json
{"event":"view","entityType":"user","entityId":"user1","targetEntityType":"item","targetEntityId":"1"}
{"event":"view","entityType":"user","entityId":"user2","targetEntityType":"item","targetEntityId":"2"}
```
```bash
pio import --appid <your_appid> --input ~/events.json
```
- Build, train, deploy
```bash
pio build
pio train
pio deploy
```

- Use the engine
```bash
curl -H "Content-Type: application/json" -d '{ "user": "user1", "num": 4, "unseenOnly":false, "blackList":["1"]}' http://localhost:8000/queries.json
```

##Customization Description
####Method of generating recommendations
The original template accepts an item as input and finds the other items with the most similar rating vectors in the predicted matrix. This customized version calculates trains the matrix in the same way but takes a user as input and returns the items with the highest predicted rating. This is how recommendations in the [basic recommendation template](https://templates.prediction.io/PredictionIO/template-scala-parallel-recommendation) are made.

####Item Properties
This template adds item properties. An item now has a title and category. [PredictionIO example code and guide](https://github.com/PredictionIO/PredictionIO/blob/develop/examples/scala-parallel-similarproduct/add-and-return-item-properties/README.md) was followed to make this change to the code

####No need to set users
In the original template, a user needed to be "set" before sending events for that user. This customization does not have that, only items need to be declared before sending events that use them. [PredictionIO example code and guide](https://github.com/PredictionIO/PredictionIO/tree/develop/examples/scala-parallel-similarproduct/no-set-user) was used to make this change.

####Only unseen items option
Users will only be recommended items which they have not previously viewed (according to the view events in the event database), if the query provides `unseenOnly:true`. [A PredictionIO guide](https://docs.prediction.io/templates/recommendation/blacklist-items/) and code from the [ecommerce template](https://github.com/PredictionIO/template-scala-parallel-ecommercerecommendation) was used as example.

####Blacklist
Standard blacklist, passed as array in the query. Used a method similar to what is described in [a predictionio guide](https://docs.prediction.io/templates/recommendation/blacklist-items/).
