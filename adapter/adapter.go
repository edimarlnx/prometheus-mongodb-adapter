package adapter

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type timeSerieDB struct {
	Name    string    `bson:"name,omitempty"`
	Labels  []*label  `bson:"labels,omitempty"`
	Samples []*sample `bson:"samples,omitempty"`
}

type timeSeries struct {
	Labels  []*label  `bson:"labels,omitempty"`
	Samples []*sample `bson:"samples,omitempty"`
}

type label struct {
	Name  string `bson:"name,omitempty"`
	Value string `bson:"value,omitempty"`
}

type sample struct {
	Timestamp int64   `bson:"timestamp"`
	Value     float64 `bson:"value"`
}

// MongoDBAdapter is an implemantation of prometheus remote stprage adapter for MongoDB
type MongoDBAdapter struct {
	cs     *connstring.ConnString
	client *mongo.Client
	c      *mongo.Collection
}

func createIndex(coll *mongo.Collection) {
	indexName := "name"
	collIdx := mongo.IndexModel{Keys: bson.D{{"name", 1}}, Options: &options.IndexOptions{Name: &indexName}}
	_, err := coll.Indexes().CreateOne(context.TODO(), collIdx)
	if err != nil {
		panic(err)
	}
	indexName = "timestamp"
	collIdx = mongo.IndexModel{Keys: bson.D{{"samples.timestamp", 1}}, Options: &options.IndexOptions{Name: &indexName}}
	_, err = coll.Indexes().CreateOne(context.TODO(), collIdx)
	if err != nil {
		panic(err)
	}
}

// New provides a MongoDBAdapter after initialization
func New(urlString, database, collection string) (*MongoDBAdapter, error) {
	if urlString == "" {
		log.Fatal("You must set your 'MONGODB_URI' environment variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}
	cs, err := connstring.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if cs.Database == "" {
		cs.Database = database
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(cs.String()))
	if err != nil {
		panic(err)
	}
	coll := client.Database(cs.Database).Collection(collection)

	createIndex(coll)

	return &MongoDBAdapter{
		cs:     &cs,
		client: client,
		c:      coll,
	}, nil
}

// Close closes the connection with MongoDB
func (p *MongoDBAdapter) Close() {
	if err := p.client.Disconnect(context.TODO()); err != nil {
		panic(err)
	}
}

// Run serves with http listener
func (p *MongoDBAdapter) Run(address string) error {
	router := httprouter.New()
	router.POST("/write", p.handleWriteRequest)
	router.POST("/read", p.handleReadRequest)
	return http.ListenAndServe(address, handlers.RecoveryHandler()(handlers.LoggingHandler(os.Stdout, router)))
}

func (p *MongoDBAdapter) handleWriteRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if !p.handleAuthRequest(w, r, params) {
		return
	}
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, ts := range req.Timeseries {
		mongoTS := &timeSerieDB{
			Labels:  []*label{},
			Samples: []*sample{},
		}
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				mongoTS.Name = l.Value
			}
			mongoTS.Labels = append(mongoTS.Labels, &label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		for _, s := range ts.Samples {
			mongoTS.Samples = append(mongoTS.Samples, &sample{
				Timestamp: s.Timestamp,
				Value:     s.Value,
			})
		}
		if _, err := p.c.InsertOne(context.TODO(), mongoTS); err != nil {
			logrus.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

func (p *MongoDBAdapter) handleReadRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if !p.handleAuthRequest(w, r, params) {
		return
	}
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := []*prompb.QueryResult{}
	for _, q := range req.Queries {

		query := map[string]interface{}{
			"samples": map[string]interface{}{
				"$elemMatch": map[string]interface{}{
					"timestamp": map[string]interface{}{
						"$gte": q.StartTimestampMs,
						"$lte": q.EndTimestampMs,
					},
				},
			},
		}
		var metricName string
		if q.Matchers != nil && len(q.Matchers) > 0 {
			matcher := []map[string]interface{}{}
			for _, m := range q.Matchers {
				if m.Name == "__name__" {
					metricName = m.Value
					continue
				}
				switch m.Type {
				case prompb.LabelMatcher_EQ:
					matcher = append(matcher, map[string]interface{}{
						"$elemMatch": map[string]interface{}{
							m.Name: m.Value,
						},
					})
				case prompb.LabelMatcher_NEQ:
					matcher = append(matcher, map[string]interface{}{
						"$elemMatch": map[string]interface{}{
							m.Name: map[string]interface{}{
								"$ne": m.Value,
							},
						},
					})
				case prompb.LabelMatcher_RE:
					matcher = append(matcher, map[string]interface{}{
						"$elemMatch": map[string]interface{}{
							m.Name: map[string]interface{}{
								"$regex": m.Value,
							},
						},
					})
				case prompb.LabelMatcher_NRE:
					matcher = append(matcher, map[string]interface{}{
						"$elemMatch": map[string]interface{}{
							m.Name: map[string]interface{}{
								"$not": map[string]interface{}{
									"$regex": m.Value,
								},
							},
						},
					})
				}
			}
			if len(matcher) > 0 {
				query["labels"] = map[string]interface{}{
					"$all": matcher,
				}
			}
			query["name"] = metricName
		}

		cursor, err := p.c.Find(context.TODO(), query, &options.FindOptions{
			Projection: map[string]int32{
				"samples": 1,
				"labels":  1,
			},
		})
		if err != nil {
			panic(err)
		}
		defer cursor.Close(context.TODO())

		var timeSeries []*prompb.TimeSeries
		cursor.All(context.TODO(), &timeSeries)
		if err != nil {
			log.Fatal(err)
		}

		results = append(results, &prompb.QueryResult{
			Timeseries: timeSeries,
		})
	}
	resp := &prompb.ReadResponse{
		Results: results,
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
