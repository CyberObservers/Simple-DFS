package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Configuration struct {
	Master struct {
		IP   string `xml:"ip"`
		Port string `xml:"port"`
	} `xml:"master"`
	StorageServers struct {
		Servers []struct {
			IP        string `xml:"ip"`
			Port      string `xml:"port"`
			Directory string `xml:"directory"`
		} `xml:"server"`
	} `xml:"storageServers"`
}

func extractChunkIndex(fileName string) (int, error) {
	// Define a regular expression to find the last number in the string
	re := regexp.MustCompile(`_(\d+)$`)

	// Find the matches
	matches := re.FindStringSubmatch(fileName)
	if len(matches) < 2 {
		return 0, fmt.Errorf("no chunk index found in file name: %s", fileName)
	}

	// Convert the matched string to an integer
	index, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid chunk index in file name: %s", fileName)
	}

	return index, nil
}

func loadConfig(configPath string) (Configuration, error) {
	var config Configuration
	xmlFile, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	defer func(xmlFile *os.File) {
		err := xmlFile.Close()
		if err != nil {
			fmt.Println("Error closing file")
		}
	}(xmlFile)
	byteValue, _ := io.ReadAll(xmlFile)
	err = xml.Unmarshal(byteValue, &config)
	if err != nil {
		return Configuration{}, err
	}
	return config, nil
}

func splitFile(filePath string, chunkSize int) ([][]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file")
			return
		}
	}(file)

	var chunks [][]byte
	buf := make([]byte, chunkSize)
	for {
		n, err := file.Read(buf)

		// fatal error
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n > 0 {
			// Create a new slice to avoid overwriting
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			chunks = append(chunks, chunk)
		}
		if err == io.EOF {
			break
		}
	}
	return chunks, nil
}

func uploadFile(masterIP, masterPort, filePath string, chunkSize int) error {
	chunks, err := splitFile(filePath, chunkSize)
	if err != nil {
		return err
	}

	chunkNames := make([]string, len(chunks))
	for i := range chunks {
		chunkNames[i] = fmt.Sprintf("%s_chunk_%d", filepath.Base(filePath), i)
	}

	uploadRequest := map[string]interface{}{
		"fileName": filepath.Base(filePath),
		"chunks":   chunkNames,
	}

	requestBody, _ := json.Marshal(uploadRequest)
	resp, err := http.Post(fmt.Sprintf("http://%s:%s/upload", masterIP, masterPort), "application/json", bytes.NewReader(requestBody))
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body")
		}
	}(resp.Body)

	var uploadURLs map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&uploadURLs); err != nil {
		return err
	}

	for i, chunk := range chunks {
		uploadURL := uploadURLs[chunkNames[i]]
		resp, err := http.Post(uploadURL+"?chunk="+chunkNames[i], "application/octet-stream", bytes.NewReader(chunk))
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func downloadFile(masterIP, masterPort, fileName string, destPath string) error {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/download?fileName=%s", masterIP, masterPort, fileName))
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body")
		}
	}(resp.Body)

	var downloadURLs map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&downloadURLs); err != nil {
		return err
	}

	type fileChunk struct {
		index int
		data  []byte
	}
	var fileChunks []fileChunk

	for i, url := range downloadURLs {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				fmt.Println("Error closing body")
			}
		}(resp.Body)

		chunk, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		thisIndex, err := extractChunkIndex(i)
		if err != nil {
			panic("Error converting index")
		}

		fileChunks = append(fileChunks, fileChunk{index: thisIndex, data: chunk})
	}

	// Sort the chunks by their index to ensure correct order
	sort.Slice(fileChunks, func(i, j int) bool {
		return fileChunks[i].index < fileChunks[j].index
	})

	var fileData []byte
	for _, chunk := range fileChunks {
		fileData = append(fileData, chunk.data...)
	}

	err = os.WriteFile(destPath, fileData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func deleteFile(masterIP, masterPort, fileName string) error {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/download?fileName=%s", masterIP, masterPort, fileName))
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body")
		}
	}(resp.Body)

	var deleteURLs map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&deleteURLs); err != nil {
		return err
	}

	for _, url := range deleteURLs {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				fmt.Println("Error closing body")
			}
		}(resp.Body)
	}

	return nil
}

func listFiles(masterIP, masterPort string) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/list", masterIP, masterPort))
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body")
		}
	}(resp.Body)

	var fileList []string
	if err := json.NewDecoder(resp.Body).Decode(&fileList); err != nil {
		return nil, err
	}

	return fileList, nil
}

func main() {
	config, err := loadConfig("config.xml")
	if err != nil {
		fmt.Println("Error loading configuration:", err)
		return
	}

	commands := make(map[string]func(args []string))

	commands["upload"] = func(args []string) {
		if len(args) < 1 {
			fmt.Println("Usage: upload <path>")
			return
		}
		filePath := args[0]
		err := uploadFile(config.Master.IP, config.Master.Port, filePath, 1024)
		if err != nil {
			fmt.Println("Error uploading file:", err)
		} else {
			fmt.Println("File uploaded successfully.")
		}
	}

	commands["download"] = func(args []string) {
		if len(args) < 2 {
			fmt.Println("Usage: download <path> <dst path>")
			return
		}
		filePath := args[0]
		localPath := args[1]
		err := downloadFile(config.Master.IP, config.Master.Port, filepath.Base(filePath), localPath)
		if err != nil {
			fmt.Println("Error downloading file:", err)
		} else {
			fmt.Println("File downloaded successfully.")
		}
	}

	commands["delete"] = func(args []string) {
		if len(args) < 1 {
			fmt.Println("Usage: delete <path>")
			return
		}
		filePath := args[0]
		err := deleteFile(config.Master.IP, config.Master.Port, filePath)
		if err != nil {
			fmt.Println("Error deleting file:", err)
		} else {
			fmt.Println("File deleted successfully.")
		}
	}

	commands["ls"] = func(args []string) {
		files, err := listFiles(config.Master.IP, config.Master.Port)
		if err != nil {
			fmt.Println("Error listing files:", err)
		} else {
			for _, file := range files {
				fmt.Println(file)
			}
		}
	}

	commands["quit"] = func(args []string) {
		fmt.Println("Exiting...")
		os.Exit(0)
	}

	commands["help"] = func(args []string) {
		fmt.Println("Available commands:")
		for cmd := range commands {
			fmt.Println(" -", cmd)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Client Machine >> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		parts := strings.SplitN(input, " ", 2)

		var args []string
		command := parts[0]
		if len(parts) > 1 {
			args = strings.Split(parts[1], " ")
		}

		if handler, found := commands[command]; found {
			handler(args)
		} else {
			fmt.Println("Unknown command:", command)
		}
	}
}
