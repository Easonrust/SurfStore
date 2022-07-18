package surfstore

import (
	context "context"
	"errors"
	"log"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	Mutex          *sync.RWMutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// read lock
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	copyMap := make(map[string]*FileMetaData)
	for k, v := range m.FileMetaMap {
		copyMap[k] = v
	}
	fileInfoMap := &FileInfoMap{FileInfoMap: copyMap}
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// write lock
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	var latestVersion *Version
	filename := fileMetaData.Filename
	old_fileMetaData, ok := m.FileMetaMap[filename]
	var err error
	// check if the file exist in the map
	if ok {
		if old_fileMetaData.Version+1 == fileMetaData.Version {
			m.FileMetaMap[filename] = fileMetaData
			latestVersion = &Version{Version: fileMetaData.Version}
			log.Println("metastore service update new file meta")
		} else {
			err = errors.New("the version trying to store is too old")
		}
	} else {
		log.Println("metastore service upload a new file meta")
		m.FileMetaMap[filename] = fileMetaData
		latestVersion = &Version{Version: fileMetaData.Version}
	}
	return latestVersion, err
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// read lock
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	// add some err check
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
		Mutex:          &sync.RWMutex{},
	}
}
