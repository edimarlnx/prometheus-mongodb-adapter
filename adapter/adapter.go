package adapter

import (
	"context"
	"fmt"
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
	"io"
	"log"
	"net/http"
	"os"
)

type timeSeries struct {
	Labels             map[string]string `bson:"labels,omitempty"`
	Samples            []*sample         `bson:"samples,omitempty"`
	SamplesMinDateTime int64             `bson:"samplesMinDateTime,omitempty"`
	SamplesMaxDateTime int64             `bson:"samplesMaxDateTime,omitempty"`
}

type sample struct {
	Timestamp int64   `bson:"timestamp"`
	Value     float64 `bson:"value"`
}

// MongoDBAdapter is an implemantation of prometheus remote stprage adapter for MongoDB
type MongoDBAdapter struct {
	client *mongo.Client
	c      *mongo.Collection
}

func createIndex(coll *mongo.Collection) {
	indexName := "name"
	collIdx := mongo.IndexModel{Keys: bson.D{{"labels.__name__", 1}}, Options: &options.IndexOptions{Name: &indexName}}
	_, err := coll.Indexes().CreateOne(context.TODO(), collIdx)
	if err != nil {
		logrus.Fatal(err)
	}
	indexName = "samplesMinDateTime"
	collIdx = mongo.IndexModel{Keys: bson.D{{"samplesMinDateTime", 1}}, Options: &options.IndexOptions{Name: &indexName}}
	_, err = coll.Indexes().CreateOne(context.TODO(), collIdx)
	if err != nil {
		logrus.Fatal(err)
	}
}

// New provides a MongoDBAdapter after initialization
func New(urlString, database, collection string) (*MongoDBAdapter, error) {
	if urlString == "" {
		log.Fatal("You must set your 'MONGO_URI' environment variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}
	cs, err := connstring.Parse(urlString)
	if err != nil {
		logrus.Fatal(err)
	}
	if cs.Database == "" {
		cs.Database = database
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(cs.String()))
	if err != nil {
		logrus.Fatal(err)
	}
	coll := client.Database(cs.Database).Collection(collection)

	createIndex(coll)

	return &MongoDBAdapter{
		client: client,
		c:      coll,
	}, nil
}

// Close closes the connection with MongoDB
func (p *MongoDBAdapter) Close() {
	if err := p.client.Disconnect(context.TODO()); err != nil {
		logrus.Fatal(err)
	}
}

// Run serves with http listener
func (p *MongoDBAdapter) Run(address string) error {
	router := httprouter.New()
	router.GET("/_health", p.handleHealthRequest)
	router.POST("/api/v1/write", p.handleWriteRequest)
	router.POST("/api/v1/read", p.handleReadRequest)
	return http.ListenAndServe(address, handlers.RecoveryHandler()(handlers.LoggingHandler(os.Stdout, router)))
}

func (p *MongoDBAdapter) handleWriteRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if !p.handleAuthRequest(w, r, params) {
		return
	}
	compressed, err := io.ReadAll(r.Body)
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
		mongoTS := &timeSeries{
			Labels:  map[string]string{},
			Samples: []*sample{},
		}
		for _, l := range ts.Labels {
			mongoTS.Labels[l.Name] = l.Value
		}
		for _, s := range ts.Samples {
			if mongoTS.SamplesMinDateTime == 0 || s.Timestamp < mongoTS.SamplesMinDateTime {
				mongoTS.SamplesMinDateTime = s.Timestamp
			}
			if mongoTS.SamplesMaxDateTime == 0 || s.Timestamp > mongoTS.SamplesMaxDateTime {
				mongoTS.SamplesMaxDateTime = s.Timestamp
			}
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
	data, err := p.loadData(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func readRequestFromBody(w http.ResponseWriter, r *http.Request) (*prompb.ReadRequest, error) {
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		logrus.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
	}
	return &req, nil
}

func (p *MongoDBAdapter) loadData(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	req, err := readRequestFromBody(w, r)
	if err != nil {
		return nil, err
	}
	var results []*prompb.QueryResult
	for _, q := range req.Queries {
		query := map[string]interface{}{
			"samplesMinDateTime": map[string]interface{}{
				"$gte": q.StartTimestampMs,
			},
			"samplesMaxDateTime": map[string]interface{}{
				"$lte": q.EndTimestampMs,
			},
		}
		if q.Matchers != nil && len(q.Matchers) > 0 {
			for _, m := range q.Matchers {
				switch m.Type {
				case prompb.LabelMatcher_EQ:
					query[fmt.Sprintf("labels.%s", m.Name)] = m.Value
				case prompb.LabelMatcher_NEQ:
					query[fmt.Sprintf("labels.%s", m.Name)] = map[string]interface{}{
						"$ne": m.Value,
					}
				case prompb.LabelMatcher_RE:
					query[fmt.Sprintf("labels.%s", m.Name)] = map[string]interface{}{
						"$regex": m.Value,
					}
				case prompb.LabelMatcher_NRE:
					query[fmt.Sprintf("labels.%s", m.Name)] = map[string]interface{}{
						"$not": map[string]interface{}{
							"$regex": m.Value,
						},
					}
				}
			}
		}
		cursor, err := p.c.Find(context.TODO(), query, &options.FindOptions{
			Sort: map[string]int32{
				"samplesMinDateTime": 1,
			},
			Projection: map[string]int32{
				"samples": 1,
				"labels":  1,
			},
		})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.TODO())

		var timeSeriesResult []*prompb.TimeSeries

		var tsDB []timeSeries
		cursor.All(context.TODO(), &tsDB)
		for _, ts := range tsDB {
			var labels []prompb.Label
			for key, value := range ts.Labels {
				labels = append(labels, prompb.Label{Name: key, Value: value})
			}
			var samples []prompb.Sample
			for _, sample := range ts.Samples {
				samples = append(samples, prompb.Sample{Timestamp: sample.Timestamp, Value: sample.Value})
			}
			timeSeriesResult = append(timeSeriesResult, &prompb.TimeSeries{
				Labels:  labels,
				Samples: samples,
			})
		}
		if err != nil {
			return nil, err
		}

		results = append(results, &prompb.QueryResult{
			Timeseries: timeSeriesResult,
		})
	}
	return proto.Marshal(&prompb.ReadResponse{
		Results: results,
	})
}
