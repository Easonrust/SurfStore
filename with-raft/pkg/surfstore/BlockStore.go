package surfstore

import (
	context "context"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	Mutex    *sync.RWMutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// read lock
	bs.Mutex.RLock()
	defer bs.Mutex.RUnlock()
	// add some err check
	return bs.BlockMap[blockHash.Hash], nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// write lock
	bs.Mutex.Lock()
	defer bs.Mutex.Unlock()
	// add some err check
	hashString := GetBlockHashString(block.BlockData)
	bs.BlockMap[hashString] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// read lock
	bs.Mutex.RLock()
	defer bs.Mutex.RUnlock()
	// add some err check
	var blockHashesOut BlockHashes
	for _, blockHash := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[blockHash]
		if ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, blockHash)
		}
	}

	return &blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
		Mutex:    &sync.RWMutex{},
	}
}
