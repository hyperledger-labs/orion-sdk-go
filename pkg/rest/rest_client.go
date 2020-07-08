package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/server"
)

// Execute REST calls to DB server
type Client struct {
	RawURL     string
	BaseURL    *url.URL
	UserAgent  string
	httpClient *http.Client
}

type ResponseErr struct {
	Error string `json:"error,omitempty"`
}

func NewRESTClient(rawurl string) (*Client, error) {
	res := &Client{}
	var err error
	res.RawURL = rawurl
	res.BaseURL, err = url.Parse(rawurl)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing url %s", rawurl)
	}
	res.httpClient = http.DefaultClient
	return res, nil
}

func (c *Client) GetStatus(ctx context.Context, in *api.GetStatusQueryEnvelope) (*api.GetStatusResponseEnvelope, error) {
	rel := &url.URL{Path: fmt.Sprintf("/db/%s", in.Payload.DBName)}

	resp, err := c.executeGetRequest(ctx, rel, in.GetPayload().GetUserID(), in.GetSignature())
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &api.GetStatusResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetState(ctx context.Context, in *api.GetStateQueryEnvelope) (*api.GetStateResponseEnvelope, error) {
	rel := &url.URL{Path: fmt.Sprintf("/db/%s/state/%s", in.Payload.DBName, in.Payload.Key)}

	resp, err := c.executeGetRequest(ctx, rel, in.GetPayload().GetUserID(), in.GetSignature())
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &api.GetStateResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) SubmitTransaction(ctx context.Context, in *api.TransactionEnvelope) (*api.ResponseEnvelope, error) {
	rel := &url.URL{Path: "/tx"}
	u := c.BaseURL.ResolveReference(rel)

	var buf io.ReadWriter
	if in != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(in)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}
	res := new(api.ResponseEnvelope)
	err = json.NewDecoder(resp.Body).Decode(res)
	if res.Data == nil {
		return nil, nil
	}
	return res, err
}

func (c *Client) executeGetRequest(ctx context.Context, relUrl *url.URL, userID string, signature []byte) (*http.Response, error){
	u := c.BaseURL.ResolveReference(relUrl)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set(server.UserHeader, userID)
	req.Header.Set(server.SignatureHeader, base64.StdEncoding.EncodeToString(signature))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}
	return resp, nil
}
