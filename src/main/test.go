package main

import (
	"fmt"
	"os"
)

func main() {
	//open, err := os.Open("src/main/pg-being_ernest.txt")
	//str := "../pg-being_ernest.txt"
	//after := strings.SplitAfter(str, "/")
	//fmt.Println(after[1])

	file, err := os.Open("pg-being_ernest.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	bytesread, err := file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("bytes read: ", bytesread)
	fmt.Println("bytestream to string: ", string(buffer))
}
