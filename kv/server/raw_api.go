package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawGetResponse)
	resp.XXX_NoUnkeyedLiteral = req.XXX_NoUnkeyedLiteral
	resp.XXX_unrecognized = req.XXX_unrecognized
	resp.XXX_sizecache = req.XXX_sizecache

	key, cf := req.GetKey(), req.GetCf()
	r, err := server.storage.Reader(nil)
	defer r.Close()

	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	val, err := r.GetCF(cf, key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := new(kvrpcpb.RawPutResponse)
	resp.XXX_NoUnkeyedLiteral = req.XXX_NoUnkeyedLiteral
	resp.XXX_unrecognized = req.XXX_unrecognized
	resp.XXX_sizecache = req.XXX_sizecache

	// generate modify batch
	batch := make([]storage.Modify, 1)
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	batch[0] = storage.Modify{Data: put}

	err := server.storage.Write(nil, batch)

	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := new(kvrpcpb.RawDeleteResponse)
	resp.XXX_NoUnkeyedLiteral = req.XXX_NoUnkeyedLiteral
	resp.XXX_unrecognized = req.XXX_unrecognized
	resp.XXX_sizecache = req.XXX_sizecache

	// generate modify patch
	batch := make([]storage.Modify, 1)
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	batch[0] = storage.Modify{Data: del}

	err := server.storage.Write(nil, batch)

	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	resp.XXX_NoUnkeyedLiteral = req.XXX_NoUnkeyedLiteral
	resp.XXX_unrecognized = req.XXX_unrecognized
	resp.XXX_sizecache = req.XXX_sizecache

	reader, err := server.storage.Reader(nil)
	defer reader.Close()

	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	start := req.GetStartKey()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	iter.Seek(start)
	limit := req.GetLimit()

	var pairs []*kvrpcpb.KvPair
	var cnt uint32 = 0
	item := iter.Item()

	for iter.Valid() && cnt < limit {
		pair := new(kvrpcpb.KvPair)
		pair.Key = item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, err
		}
		pair.Value = val

		pairs = append(pairs, pair)
		iter.Next()
		item = iter.Item()
		cnt += 1
	}

	resp.Kvs = pairs

	return resp, nil
}
