package storeutils

import (
	"math"
	"math/rand"
	"time"

	"google.golang.org/cloud/storage"
)

// Copied from cloudstorage
var GCSRetries = 10

//backoff sleeps a random amount so we can.
//retry failed requests using a randomized exponential backoff:
//wait a random period between [0..1] seconds and retry; if that fails,
//wait a random period between [0..2] seconds and retry; if that fails,
//wait a random period between [0..4] seconds and retry, and so on,
//with an upper bounds to the wait period being 16 seconds.
//http://play.golang.org/p/l9aUHgiR8J
func backoff(try int) {
	nf := math.Pow(2, float64(try))
	nf = math.Max(1, nf)
	nf = math.Min(nf, 16)
	r := rand.Int31n(int32(nf))
	d := time.Duration(r) * time.Second
	time.Sleep(d)
}

func concatGCSObjects(a, b *storage.ObjectList) *storage.ObjectList {
	for _, obj := range b.Results {
		a.Results = append(a.Results, obj)
	}
	for _, prefix := range b.Prefixes {
		a.Prefixes = append(a.Prefixes, prefix)
	}
	return a
}
