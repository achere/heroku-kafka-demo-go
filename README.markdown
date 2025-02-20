# WMS POC
## Provisioning Kafka

Install the Kafka CLI plugin:

```
$ heroku plugins:install heroku-kafka
```

Attach kafka addon:

```
$ heroku addons:create heroku-kafka:basic-0 -a achere-wms
```

Create topics:

```
heroku kafka:topics:create stock-updates -a achere-wms
heroku kafka:topics:create low-stock-alerts -a achere-wms
```

Create the consumer group:

```
heroku kafka:consumer-groups:create wms -a achere-wms
```

Deploy to Heroku and open the app:

```
$ git push heroku main
$ heroku open
```
