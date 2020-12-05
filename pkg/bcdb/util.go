package bcdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gogo/protobuf/proto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
)

func handleRequest(r requester, rawurl string, query proto.Message, res proto.Message) error {
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	restURL := r.selectReplica().ResolveReference(parsedURL).String()
	response, err := r.client().Query(context.TODO(), restURL, query)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("error handling request, server returned %s", response.Status))
	}
	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		r.log().Errorf("failed to decode json response, due to", err)
		return err
	}
	return nil
}

type requester interface {
	selectReplica() *url.URL
	client() RestClient
	log() *logger.SugarLogger
}
