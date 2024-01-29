package main

import (
	"fmt"
	"ksql/disc"
	"log"
	"os"
)

func main() {
	f1, err := os.OpenFile("test_table", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}

	dm := disc.NewDiskManager(f1)
	// dm.WritePageData(nextID, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17})
	fmt.Println("res: ", dm.ReadPageData(disc.PageID(1)))
}
