package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iterWrite engine_util.DBIterator
	txn       *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	ret := &Scanner{txn: txn}
	ret.iterWrite = txn.Reader.IterCF(engine_util.CfWrite)
	ret.iterWrite.Seek(EncodeKey(startKey, txn.StartTS))
	// Iterate to next key
	curKey := startKey
	for ; ret.iterWrite.Valid(); ret.iterWrite.Next() {
		// Old version of the key
		if bytes.Equal(curKey, DecodeUserKey(ret.iterWrite.Item().Key())) {
			continue
		}
		// Version is higher
		if decodeTimestamp(ret.iterWrite.Item().Key()) > ret.txn.StartTS {
			continue
		}
		writeVal, err := ret.iterWrite.Item().Value()
		if err != nil {
			panic(err.Error())
		}
		write, err := ParseWrite(writeVal)
		if err != nil {
			panic(err.Error())
		}
		if write.Kind != WriteKindPut {
			curKey = DecodeUserKey(ret.iterWrite.Item().Key())
			continue
		}
		// Find the postition
		break
	}
	return ret
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iterWrite.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iterWrite.Valid() {
		return nil, nil, nil
	}
	// Get key, value
	retKey := DecodeUserKey(scan.iterWrite.Item().Key())
	writeVal, err := scan.iterWrite.Item().Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return nil, nil, err
	}
	retVal, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(retKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	// Iterate to next key
	curKey := retKey
	for ; scan.iterWrite.Valid(); scan.iterWrite.Next() {
		// Old version of the key
		if bytes.Equal(curKey, DecodeUserKey(scan.iterWrite.Item().Key())) {
			continue
		}
		// Version is higher
		if decodeTimestamp(scan.iterWrite.Item().Key()) > scan.txn.StartTS {
			continue
		}
		writeVal, err := scan.iterWrite.Item().Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(writeVal)
		if err != nil {
			return nil, nil, err
		}
		if write.Kind != WriteKindPut {
			curKey = DecodeUserKey(scan.iterWrite.Item().Key())
			continue
		}
		// Find the postition
		break
	}
	return retKey, retVal, nil
}
