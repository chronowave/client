package client

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec"
)

type Client struct {
	clt flight.Client
}

func New(uri string) (*Client, error) {
	clt, err := flight.NewClientWithMiddleware(uri, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return &Client{
		clt: clt,
	}, err
}

func (c *Client) CreateFlight(ctx context.Context, name string, schema []byte) error {
	body, err := proto.Marshal(&codec.FlightSchemaRequest{
		Flight: name,
		Schema: schema,
	})
	if err != nil {
		return err
	}

	createFlight := flight.Action{
		Type: codec.FlightServiceAction_CreateFlight.String(),
		Body: body,
	}

	action, err := c.clt.DoAction(ctx, &createFlight)
	if err != nil {
		return err
	}

	_, err = action.Recv()
	return err
}

// Query returns JSON document
func (c *Client) Query(ctx context.Context, qry string, v any) error {
	get, err := c.clt.DoGet(ctx, &flight.Ticket{Ticket: []byte(qry)})
	if err != nil {
		return err
	}

	resp, err := get.Recv()
	if err != nil {
		return err
	}

	reader, err := ipc.NewReader(bytes.NewReader(resp.DataBody))
	if err != nil {
		return err
	}
	defer reader.Release()

	err = reader.Err()
	if err != nil {
		return err
	}

	for reader.Next() {
		record := reader.Record()
		defer record.Release()
		return UnmarshalRecord(record, v)
	}

	return nil
}

func (c *Client) UploadData(ctx context.Context, flightName string, data []byte) error {
	flightDesc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{flightName},
	}

	loader, err := c.clt.DoPut(ctx)
	if err != nil {
		return err
	}
	defer loader.CloseSend()

	err = loader.Send(&flight.FlightData{
		FlightDescriptor: flightDesc,
		DataBody:         data,
	})

	if err != nil {
		return err
	}

	resp, err := loader.Recv()
	if err != nil {
		return err
	}

	var pr codec.PutResult
	err = proto.Unmarshal(resp.AppMetadata, &pr)
	if err != nil {
		return err
	}

	sb := strings.Builder{}
	for i, e := range pr.Error {
		if len(e) > 0 {
			if sb.Len() > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("document[%d] err: %v", i, e))
		}
	}

	if sb.Len() > 0 {
		return fmt.Errorf(sb.String())
	}

	return nil
}
