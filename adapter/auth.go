package adapter

import (
	"context"
	"github.com/julienschmidt/httprouter"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"net/http"
	"os"
	"strings"
)

func GetEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}

var authCodeTest = GetEnv("AUTH_TOKEN", "")

func (p *MongoDBAdapter) handleHealthRequest(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	err := p.client.Ping(context.TODO(), readpref.PrimaryPreferred())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (p *MongoDBAdapter) handleAuthRequest(w http.ResponseWriter, r *http.Request, _ httprouter.Params) bool {
	if authCodeTest == "" {
		return true
	}
	apiKey := strings.Replace(r.Header.Get("authorization"), "Bearer ", "", 1)
	if apiKey != authCodeTest {
		w.WriteHeader(http.StatusUnauthorized)
		return false
	}
	return true
}
