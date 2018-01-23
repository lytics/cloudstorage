package cloudstorage

import "sort"

// Filter func type definition for filtering objects
type Filter func(objects Objects) Objects

// Query used to query the cloud source. The primary query is a prefix query like
// `ls /my-csv-files/baseball/*`.
type Query struct {
	Delimiter string   // Delimiter is most likely "/"
	Prefix    string   // prefix (directory) to search for or object name if one file
	Cursor    string   // Cursor if provided is a start next page fetch bookmark.
	Filters   []Filter // Applied to the result sets to filter out Objects (i.e. remove objects by extension)
}

// NewQuery create a query for finding files under given prefix
func NewQuery(prefix string) Query {
	return Query{
		Prefix: prefix,
	}
}

// NewQueryForFolders create a query for finding Folders under given path
func NewQueryForFolders(folderPath string) Query {
	return Query{
		Delimiter: "/",
		Prefix:    folderPath,
	}
}

// AddFilter adds a post prefix query, that can be used to alter results set from the
// prefix query.
func (q *Query) AddFilter(f Filter) *Query {
	if q.Filters == nil {
		q.Filters = make([]Filter, 0)
	}
	q.Filters = append(q.Filters, f)
	return q
}

// Sorted added a sort Filter to the filter chain, if its not the last call while building your query,
// Then sorting is only guaranteed for the next filter in the chain.
func (q *Query) Sorted() *Query {
	q.AddFilter(ObjectSortFilter)
	return q
}

// ApplyFilters is called as the last step in store.List() to filter out the results before they are returned.
func (q *Query) ApplyFilters(objects Objects) Objects {
	for _, f := range q.Filters {
		objects = f(objects)
	}
	return objects
}

var ObjectSortFilter = func(objs Objects) Objects {
	sort.Stable(objs)
	return objs
}
