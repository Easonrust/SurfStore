package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	for {
		for idx := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)

			log.Println(idx)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fm, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			*serverFileInfoMap = fm.FileInfoMap
			return conn.Close()
		}
	}
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for {
		for idx := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			v, err := c.UpdateFile(ctx, fileMetaData)
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			*latestVersion = v.Version
			// close the connection
			return conn.Close()
		}
	}
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for {
		for idx := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			*blockStoreAddr = addr.Addr
			// close the connection
			return conn.Close()
		}
	}
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
