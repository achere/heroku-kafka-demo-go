# WMS POC


## Provisioning Addons

### Kafka
Install the Kafka CLI plugin:

```sh
heroku plugins:install heroku-kafka
```

Attach kafka addon:

```sh
heroku addons:create heroku-kafka:basic-0 -a $APP_NAME
```

Create topics:

```sh
heroku kafka:topics:create stock-updates -a $APP_NAME
heroku kafka:topics:create low-stock-alerts -a $APP_NAME
```

Create the consumer group:

```sh
heroku kafka:consumer-groups:create wms -a $APP_NAME
```

### Postgres

```sh
 heroku addons:create heroku-postgresql:essential-0 -a $APP_NAME
```

### Redis

```sh
heroku addons:create heroku-redis:mini -a $APP_NAME
```

## Testing POC

```sh
heroku kafka:topics:write ${KAFKA_PREFIX}stock-updates -a $APP_NAME '{"product_id":1,"warehouse_id":1,"stock_delta":-7}'
```

## Deprovisioning addons

```sh
heroku addons:detach kafka -a wms-producer
heroku addons:destroy -a $APP_NAME kafka --confirm $APP_NAME
heroku addons:destroy -a $APP_NAME --confirm $APP_NAME heroku-postgresql
heroku addons:destroy -a $APP_NAME --confirm $APP_NAME heroku-redis
```
