package main

import (
	"context"
	"fmt"
	"os"

	"github.com/araddon/gou"
	"google.golang.org/api/iterator"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/azure"
)

/*

# to use azure tests ensure you have exported

export AZURE_KEY="aaa"
export AZURE_PROJECT="bbb"
export AZURE_BUCKET="cloudstorageunittests"

*/

func main() {
	conf := &cloudstorage.Config{
		Type:       azure.StoreType,
		AuthMethod: azure.AuthKey,
		Bucket:     os.Getenv("AZURE_BUCKET"),
		Project:    os.Getenv("AZURE_PROJECT"),
		TmpDir:     "/tmp/localcache/azure",
		Settings:   make(gou.JsonHelper),
	}

	conf.Settings[azure.ConfKeyAuthKey] = os.Getenv("AZURE_KEY")

	// Should error with empty config
	store, err := cloudstorage.NewStore(conf)
	if err != nil {
		fmt.Println("Could not get azure store ", err)
		os.Exit(1)
	}

	folders, err := store.Folders(context.Background(), cloudstorage.NewQueryForFolders(""))
	if err != nil {
		fmt.Println("Could not get folders ", err)
		os.Exit(1)
	}
	for _, folder := range folders {
		fmt.Println("found folder: ", folder)
	}

	// Create a search query for all objects
	q := cloudstorage.NewQuery("")
	// Create an Iterator
	iter, err := store.Objects(context.Background(), q)
	if err != nil {
		fmt.Println("Could not get iter ", err)
		os.Exit(1)
	}

	for {
		o, err := iter.Next()
		if err == iterator.Done {
			fmt.Println("done, exiting iterator")
			break
		}
		fmt.Println("found object", o.Name())
	}
}
