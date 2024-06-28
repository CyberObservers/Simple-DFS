package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
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

type Master struct {
	mu        sync.Mutex
	fileIndex map[string][]string
	config    Configuration
}

func NewMaster(config Configuration) *Master {
	return &Master{
		fileIndex: make(map[string][]string),
		config:    config,
	}
}

func (m *Master) uploadHandler(w http.ResponseWriter, r *http.Request) {
	var request struct {
		FileName string   `json:"fileName"`
		Chunks   []string `json:"chunks"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.fileIndex[request.FileName] = request.Chunks

	response := make(map[string]string)
	for i, chunk := range request.Chunks {
		server := m.config.StorageServers.Servers[i%len(m.config.StorageServers.Servers)]
		response[chunk] = fmt.Sprintf("http://%s:%s/uploadBlock", server.IP, server.Port)
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}
}

func (m *Master) downloadHandler(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")
	if fileName == "" {
		http.Error(w, "fileName is required", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	chunks, exists := m.fileIndex[fileName]
	if !exists {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	response := make(map[string]string)
	for i, chunk := range chunks {
		server := m.config.StorageServers.Servers[i%len(m.config.StorageServers.Servers)]
		response[chunk] = fmt.Sprintf("http://%s:%s/downloadBlock?chunk=%s", server.IP, server.Port, chunk)
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		return
	}
}

func (m *Master) listHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fileList := make([]string, 0, len(m.fileIndex))
	for fileName := range m.fileIndex {
		fileList = append(fileList, fileName)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fileList)
}

func (m *Master) Run() {
	http.HandleFunc("/upload", m.uploadHandler)
	http.HandleFunc("/download", m.downloadHandler)
	http.HandleFunc("/list", m.listHandler)
	addr := fmt.Sprintf("%s:%s", m.config.Master.IP, m.config.Master.Port)
	fmt.Println("Master running on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("Error occurs when running master:", err)
	}
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

func main() {
	config, err := loadConfig("config.xml")
	if err != nil {
		fmt.Println("Error loading configuration:", err)
		return
	}
	master := NewMaster(config)
	master.Run()
}
