# prometheus-mongodb-adapter: Prometheus remote storage adapter implementation for MongoDB

Now under development

## Getting Started


## Configuration

```bash
$ prometheus-mongodb-adapter --help                                                  
NAME:
   prometheus-mongodb-adapter

OPTIONS:
   --mongo-url value, -m value   (default: "mongodb://localhost:27017/prometheus") [$MONGO_URI]
   --database value, -d value    (default: "prometheus") [$DATABASE_NAME]
   --collection value, -c value  (default: "prometheus") [$COLLECTION_NAME]
   --address value, -a value     (default: "0.0.0.0:8080") [$LISTEN_ADDRESS]
   --help, -h                    show help
   --version, -v                 print the version
```

If database name is included in `mongo-url`, `databse` argument is ignored.


## License

MIT License

### Original author

Forked from [kiragoo/prometheus-mongodb-adapter](https://github.com/kiragoo/prometheus-mongodb-adapter)
