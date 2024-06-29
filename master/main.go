package main

import (
	"GoDFS/utils"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
)

/*
TODO: 1. solve file upload with the same name, involving data update and conflict solving.
TODO: 2. metadata persistence.
TODO: 3. Support for Linux; echo off some windows.
*/

type Master struct {
	mu        sync.Mutex
	fileIndex map[string][]string
	config    utils.Configuration
}

func NewMaster(config utils.Configuration) *Master {
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

	if err := m.saveMetadata(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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

func (m *Master) deleteHandler(w http.ResponseWriter, r *http.Request) {
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

	delete(m.fileIndex, fileName)

	if err := m.saveMetadata(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := make(map[string]string)
	for i, chunk := range chunks {
		server := m.config.StorageServers.Servers[i%len(m.config.StorageServers.Servers)]
		response[chunk] = fmt.Sprintf("http://%s:%s/deleteBlock?chunk=%s", server.IP, server.Port, chunk)
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
	err := json.NewEncoder(w).Encode(fileList)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
}

func (m *Master) saveMetadata() error {
	file, err := os.Create(m.config.Master.Directory)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(m.fileIndex)
}

func (m *Master) loadMetadata() error {
	file, err := os.Open(m.config.Master.Directory)
	if err != nil {
		if os.IsNotExist(err) {
			// 如果文件不存在，说明是第一次启动
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	return decoder.Decode(&m.fileIndex)
}

func (m *Master) Run() {
	http.HandleFunc("/upload", m.uploadHandler)
	http.HandleFunc("/download", m.downloadHandler)
	http.HandleFunc("/list", m.listHandler)
	http.HandleFunc("/delete", m.deleteHandler)
	addr := fmt.Sprintf("%s:%s", m.config.Master.IP, m.config.Master.Port)

	if err := m.loadMetadata(); err != nil {
		panic(err)
	}

	fmt.Println("Master running on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("Error occurs when running master:", err)
	}
}

func main() {
	config, err := utils.LoadConfig("config.xml")
	if err != nil {
		fmt.Println("Error loading configuration:", err)
		return
	}
	master := NewMaster(config)
	master.Run()
}
