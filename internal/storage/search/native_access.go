package search

import (
	"errors"
	"net/http"
	"strings"
)

// Only known document/query mutations may pass through. A blacklist of index
// deletion paths would miss aliases, restore, lifecycle policies and plugins
// that can replace the index behind a previously issued record revision.
func validateNativeExecution(opts requestOptions) error {
	switch opts.method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return nil
	}
	parts := strings.Split(strings.Trim(opts.path, "/"), "/")
	indexed := parts[0] != "" && (!strings.HasPrefix(parts[0], "_") || parts[0] == "_all" || strings.HasPrefix(parts[0], "_all,"))
	if indexed {
		parts = parts[1:]
	}
	if nativeDataOperation(opts.method, parts, indexed) {
		return nil
	}
	return errors.New("native endpoint is not permitted: index lifecycle, alias, settings, template and other administrative mutations must use a separate database administration path")
}

func nativeDataOperation(method string, parts []string, indexed bool) bool {
	if len(parts) == 0 {
		return false
	}
	operation := parts[0]
	if indexed && len(parts) >= 2 {
		// Decoded document IDs can contain slashes. Everything after the
		// document endpoint is an ID, not an administrative action.
		switch operation {
		case "_doc":
			return method == http.MethodPut || method == http.MethodPost || method == http.MethodDelete
		case "_create":
			return method == http.MethodPut || method == http.MethodPost
		case "_update", "_explain", "_termvectors":
			return method == http.MethodPost
		}
	}
	if len(parts) == 2 {
		switch {
		case operation == "_search" && parts[1] == "scroll" && !indexed:
			return method == http.MethodPost || method == http.MethodDelete
		case operation == "_search" && parts[1] == "point_in_time":
			return (indexed && method == http.MethodPost) || (!indexed && method == http.MethodDelete)
		case (operation == "_search" || operation == "_msearch") && parts[1] == "template":
			return method == http.MethodPost
		case operation == "_validate" && parts[1] == "query":
			return method == http.MethodPost
		case operation == "_cache" && parts[1] == "clear":
			return method == http.MethodPost
		}
	}
	if len(parts) != 1 {
		return false
	}
	switch operation {
	case "_bulk", "_mget", "_msearch", "_search", "_search_shards", "_count", "_termvectors", "_mtermvectors", "_analyze", "_field_caps", "_rank_eval", "_terms_enum",
		"_refresh", "_flush", "_forcemerge", "_update_by_query", "_delete_by_query":
		return method == http.MethodPost
	case "_doc":
		return indexed && method == http.MethodPost
	case "_mapping":
		return method == http.MethodPut || method == http.MethodPost
	case "_reindex":
		return !indexed && method == http.MethodPost
	case "_pit":
		return (indexed && method == http.MethodPost) || (!indexed && method == http.MethodDelete)
	}
	return false
}
