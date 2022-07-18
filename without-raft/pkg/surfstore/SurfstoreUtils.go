package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// read the index.txt into metadata map, note that if no index.txt, the localMetaMap will be empty
	clientFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("read index.txt infomap err")
		log.Println(err)
	}

	// read the fileinfo into the map, then we can easily check if a file in the dir
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println(err)
	}
	changedfileMap := make(map[string]int)
	// 1. First we need to combine the result from base and index
	// update the clientFileInfoMap with the files in the base dir
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		fileInfo, ok := clientFileInfoMap[file.Name()]
		if ok {
			// user updates a file, this case the version should add 1
			version := fileInfo.Version
			blockHashList, changed := getAndCompareBlockHashList(file.Name(), fileInfo, client, UPDATE)
			if changed {
				clientFileInfoMap[file.Name()] = &FileMetaData{Filename: file.Name(), Version: int32(version + 1), BlockHashList: blockHashList}
				changedfileMap[file.Name()] = int(version + 1)
			}
		} else {
			// user add a file, this case the version should init 1
			version := 0
			blockHashList, _ := getAndCompareBlockHashList(file.Name(), fileInfo, client, CREATE)
			clientFileInfoMap[file.Name()] = &FileMetaData{Filename: file.Name(), Version: int32(version + 1), BlockHashList: blockHashList}
			changedfileMap[file.Name()] = int(version + 1)
		}
	}

	// we also need to check if the user has delete some
	fileMap := make(map[string]os.FileInfo)
	for _, file := range files {
		fileMap[file.Name()] = file
	}
	for k := range clientFileInfoMap {
		fileInfo := clientFileInfoMap[k]
		_, ok := fileMap[k]
		if fileInfo.BlockHashList[0] == "0" && len(fileInfo.BlockHashList) == 1 {
			continue
		}
		if !ok {
			// user delete a file, this case the version should add 1
			// todo : can I repeat deleting?
			log.Println("try to delete a file")
			version := clientFileInfoMap[k].Version
			blockHashList, _ := getAndCompareBlockHashList(fileInfo.Filename, fileInfo, client, DELETE)
			clientFileInfoMap[k] = &FileMetaData{Filename: k, Version: version + 1, BlockHashList: blockHashList}
			changedfileMap[k] = int(version + 1)
		}
	}

	// 2. First we sync with the server
	// download fileInfoMap
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		log.Println("get fileInfomap from server error")
		log.Println(err)
	}

	// check if the client has sth that server does not have or have
	for k := range clientFileInfoMap {
		fileInfo, ok := serverFileInfoMap[k]
		if ok {
			// if we find that the client version is bigger, we need to try to update
			if fileInfo.Version < clientFileInfoMap[k].Version {
				log.Println("try to update a file exist on the server")
				// first put the block	on the server
				updateFileFromClient(clientFileInfoMap[k], client)

				// second try to update the metadata on the server
				var latestVersion int32 = 1
				err := client.UpdateFile(clientFileInfoMap[k], &latestVersion)

				// user try to update the existing file, but someone may have update the server ealier, in this case update fail and return err, we need to update the local from the server
				if err != nil {
					log.Println("update a file error")
					log.Println(err)
					// download the new serverFileInfoMap
					serverFileInfoMap = make(map[string]*FileMetaData)
					err = client.GetFileInfoMap(&serverFileInfoMap)
					if err != nil {
						log.Println("download new fileInfomap from server error")
						log.Println(err)
					}
					newfileInfo := serverFileInfoMap[k]
					clientFileInfoMap[k] = &FileMetaData{Filename: newfileInfo.Filename, Version: newfileInfo.Version, BlockHashList: newfileInfo.BlockHashList}

					// download file from the server
					updateFileFromServer(newfileInfo, client)
					continue
				}

				// // else user upload the updated file, update success
				// updateFileFromClient(clientFileInfoMap[k], client)
			} else if fileInfo.Version > clientFileInfoMap[k].Version {
				// if we find that the client version is lower than the server, we need to try to update the client from the server
				// download the new serverFileInfoMap
				serverFileInfoMap = make(map[string]*FileMetaData)
				err = client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Println("download new fileInfomap from server error")
					log.Println(err)
				}
				newfileInfo := serverFileInfoMap[k]
				clientFileInfoMap[k] = &FileMetaData{Filename: newfileInfo.Filename, Version: newfileInfo.Version, BlockHashList: newfileInfo.BlockHashList}

				// download file from the server
				updateFileFromServer(newfileInfo, client)
			} else {
				// if we find that the client version is equal to the server, we only update those changed file
				_, changed := changedfileMap[k]
				if changed {
					// download the new serverFileInfoMap
					serverFileInfoMap = make(map[string]*FileMetaData)
					err = client.GetFileInfoMap(&serverFileInfoMap)
					if err != nil {
						log.Println("download new fileInfomap from server error")
						log.Println(err)
					}
					newfileInfo := serverFileInfoMap[k]
					clientFileInfoMap[k] = &FileMetaData{Filename: newfileInfo.Filename, Version: newfileInfo.Version, BlockHashList: newfileInfo.BlockHashList}

					// download file from the server
					updateFileFromServer(newfileInfo, client)
				}
			}
		} else {
			log.Println("try to upload a file not exist on the server")

			// first put the block	on the server
			uploadFileFromClient(k, client)

			//second try to update the metadata on the server
			var latestVersion int32 = 1
			// upload metadata
			err := client.UpdateFile(clientFileInfoMap[k], &latestVersion)

			// user try to upload a new file, but someone may have upload the file ealier, in this case upload fail and return err, we need to update the local from the server
			if err != nil {
				log.Println("upload a file error")
				log.Println(err)
				// download the new serverFileInfoMap
				serverFileInfoMap = make(map[string]*FileMetaData)
				err = client.GetFileInfoMap(&serverFileInfoMap)
				if err != nil {
					log.Println("download new fileInfomap from server error")
					log.Println(err)
				}
				newfileInfo := serverFileInfoMap[k]
				clientFileInfoMap[k] = &FileMetaData{Filename: newfileInfo.Filename, Version: newfileInfo.Version, BlockHashList: newfileInfo.BlockHashList}

				// download file from the server
				updateFileFromServer(newfileInfo, client)
				continue
			}
		}
	}

	// download the new serverFileInfoMap to keep the serverFileInfoMap new
	serverFileInfoMap = make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		log.Println("download new fileInfomap from server error")
		log.Println(err)
	}

	// check if the server has sth that client does not have
	for k := range serverFileInfoMap {
		_, ok := clientFileInfoMap[k]

		if ok {
			continue
		}

		log.Println("try to download a file from the server")
		// update local meta
		clientFileInfoMap[k] = &FileMetaData{Filename: k, Version: serverFileInfoMap[k].Version, BlockHashList: serverFileInfoMap[k].BlockHashList}

		// download file from the server
		updateFileFromServer(serverFileInfoMap[k], client)

	}

	err = WriteMetaFile(clientFileInfoMap, client.BaseDir)

	if err != nil {
		log.Println(err)
	}
}

func getAndCompareBlockHashList(filename string, fileInfo *FileMetaData, client RPCClient, operation int) ([]string, bool) {
	var hashList []string
	var changed bool
	changed = false
	if operation == DELETE {
		hashString := "0"
		hashList = append(hashList, hashString)
		changed = true
	} else if operation == CREATE {
		changed = true
		file, err := os.Open(ConcatPath(client.BaseDir, filename))
		if err != nil {
			log.Println(err)
		}
		defer file.Close()
		for {
			buffer := make([]byte, client.BlockSize)
			n, err := file.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println(err)
			}
			hashString := GetBlockHashString(buffer[:n])
			hashList = append(hashList, hashString)
		}
	} else if operation == UPDATE {
		file, err := os.Open(ConcatPath(client.BaseDir, filename))
		if err != nil {
			log.Println(err)
		}
		defer file.Close()
		idx := 0
		for {
			buffer := make([]byte, client.BlockSize)
			n, err := file.Read(buffer)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println(err)
			}
			hashString := GetBlockHashString(buffer[:n])
			if !changed {
				if idx < len(fileInfo.BlockHashList) && fileInfo.BlockHashList[idx] != hashString {
					changed = true
				}
			}
			idx++
			hashList = append(hashList, hashString)
		}
	}

	return hashList, changed
}

func uploadFileFromClient(filename string, client RPCClient) {
	file, err := os.Open(ConcatPath(client.BaseDir, filename))
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	var blockStoreAddr string = ""
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Println("get block store addr when upload file error")
		log.Println(err)
	}
	for {
		buffer := make([]byte, client.BlockSize)
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
		}
		var succ bool = true
		err = client.PutBlock(&Block{BlockData: buffer[:n], BlockSize: int32(n)}, blockStoreAddr, &succ)
		if err != nil {
			log.Println("put block when upload file error")
			log.Println(blockStoreAddr)
			log.Println(err)
		}
	}
}

func updateFileFromClient(fileInfo *FileMetaData, client RPCClient) {
	// when the file is deleted, we do not update it to the server blockStore
	if fileInfo.BlockHashList[0] == "0" && len(fileInfo.BlockHashList) == 1 {
		return
	}

	file, err := os.Open(ConcatPath(client.BaseDir, fileInfo.Filename))
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	var blockStoreAddr string = ""
	var blockHashesOut []string = make([]string, 0)

	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Println(err)
	}
	err = client.HasBlocks(fileInfo.BlockHashList, blockStoreAddr, &blockHashesOut)
	if err != nil {
		log.Println(err)
	}

	blockHashesOutMap := make(map[string]int)
	for i, hash := range blockHashesOut {
		blockHashesOutMap[hash] = i
	}

	log.Println("updating block")
	for {
		buffer := make([]byte, client.BlockSize)
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
		}

		// check if the block already exist on the server, if yes we do not upload that block
		_, ok := blockHashesOutMap[GetBlockHashString(buffer[:n])]
		if ok {
			continue
		}

		var succ bool = true
		err = client.PutBlock(&Block{BlockData: buffer[:n], BlockSize: int32(n)}, blockStoreAddr, &succ)
		if err != nil {
			log.Println(err)
		}
	}
}

func updateFileFromServer(fileInfo *FileMetaData, client RPCClient) {
	// when the file is deleted, we do not download it from the blockstore
	filePath := ConcatPath(client.BaseDir, fileInfo.Filename)
	if fileInfo.BlockHashList[0] == "0" && len(fileInfo.BlockHashList) == 1 {
		err := os.Remove(filePath)
		if err != nil {
			log.Println(err)
		}
		return
	}

	_, err := os.Stat(filePath)

	if !os.IsNotExist(err) {
		os.Remove(filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	blockHashList := fileInfo.BlockHashList
	var blockStoreAddr string = ""
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Println(err)
	}
	for _, blockHash := range blockHashList {
		var block Block
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Println(err)
		}
		_, err = file.Write(block.BlockData)
		if err != nil {
			log.Println(err)
		}
	}
}
