package utils

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
)

const ChunkSize = 1024 * 1024

type Configuration struct {
	Master struct {
		IP        string `xml:"ip"`
		Port      string `xml:"port"`
		Directory string `xml:"directory"`
	} `xml:"master"`
	StorageServers struct {
		Servers []struct {
			IP        string `xml:"ip"`
			Port      string `xml:"port"`
			Directory string `xml:"directory"`
		} `xml:"server"`
	} `xml:"storageServers"`
}

func LoadConfig(configPath string) (Configuration, error) {
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
