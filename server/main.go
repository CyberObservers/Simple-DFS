package main

import (
	"GoDFS/utils"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

type StorageServer struct {
	IP        string
	Port      string
	Directory string
}

func NewStorageServer(ip, port, directory string) *StorageServer {
	return &StorageServer{
		IP:        ip,
		Port:      port,
		Directory: directory,
	}
}

func (s *StorageServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	chunk := r.URL.Query().Get("chunk")
	if chunk == "" {
		http.Error(w, "chunk is required", http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(s.Directory, chunk)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = fmt.Fprintf(w, "Chunk %s uploaded successfully", chunk)
	if err != nil {
		return
	}
}

func (s *StorageServer) downloadHandler(w http.ResponseWriter, r *http.Request) {
	chunk := r.URL.Query().Get("chunk")
	if chunk == "" {
		http.Error(w, "chunk is required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(s.Directory, chunk)
	data, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(data)
	if err != nil {
		return
	}
}

func (s *StorageServer) deleteHandler(w http.ResponseWriter, r *http.Request) {
	chunk := r.URL.Query().Get("chunk")
	if chunk == "" {
		http.Error(w, "chunk is required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(s.Directory, chunk)
	err := os.Remove(filePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{
		"message": "Chunk deleted successfully",
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(jsonResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *StorageServer) Run() {
	http.HandleFunc("/uploadBlock", s.uploadHandler)
	http.HandleFunc("/downloadBlock", s.downloadHandler)
	http.HandleFunc("/deleteBlock", s.deleteHandler)
	addr := fmt.Sprintf("%s:%s", s.IP, s.Port)
	fmt.Println("Storage Server running on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("Error when running chunk server", err)
		return
	}
}

func main() {
	configPath := flag.String("config", "config.xml", "Path to configuration file")
	serverIndex := flag.Int("server", 1, "Storage server index (starting from 1)")
	flag.Parse()

	config, err := utils.LoadConfig(*configPath)
	if err != nil {
		fmt.Println("Error loading configuration:", err)
		return
	}

	if *serverIndex < 1 || *serverIndex > len(config.StorageServers.Servers) {
		fmt.Println("Invalid server index")
		return
	}

	serverConfig := config.StorageServers.Servers[*serverIndex-1]
	storageServer := NewStorageServer(serverConfig.IP, serverConfig.Port, serverConfig.Directory)
	storageServer.Run()
}
