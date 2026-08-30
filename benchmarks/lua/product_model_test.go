package luabench

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type benchmarkRating struct {
	Score       float64 `json:"score,omitempty"`
	Count       int64   `json:"count,omitempty"`
	Description string  `json:"description,omitempty"`
}

type benchmarkProductPrice struct {
	Currency  string         `json:"currency"`
	Price     float64        `json:"price"`
	Dollar    float64        `json:"dollar,omitempty"`
	Variables map[string]any `json:"variables,omitempty"`
	Date      *time.Time     `json:"date"`
}

type benchmarkImage struct {
	URL string `json:"url"`
	Key string `json:"key"`
}

type benchmarkProductSold struct {
	Sold        int64      `json:"sold,omitempty"`
	PeriodHours int64      `json:"period_hours,omitempty"`
	RecordAt    *time.Time `json:"record_at,omitempty"`
}

type benchmarkProductStock struct {
	Stock     int64          `json:"stock,omitempty"`
	Variables map[string]any `json:"variables,omitempty"`
}

type benchmarkProductComment struct {
	Score               float64    `json:"score,omitempty"`
	Title               string     `json:"title,omitempty"`
	Description         string     `json:"description,omitempty"`
	CustomerName        string     `json:"customer_name,omitempty"`
	CustomerProfile     string     `json:"customer_profile,omitempty"`
	CustomerRatingScore float64    `json:"customer_rating_score,omitempty"`
	Location            string     `json:"location,omitempty"`
	Currency            string     `json:"currency,omitempty"`
	Price               float64    `json:"price,omitempty"`
	Date                *time.Time `json:"date,omitempty"`
}

type benchmarkProductOffer struct {
	ID               string                   `json:"id"`
	UID              string                   `json:"uid"`
	UIDs             []string                 `json:"uids"`
	URL              string                   `json:"url"`
	Name             string                   `json:"name"`
	ExpiryDetectedAt *time.Time               `json:"expiry_detected_at"`
	ProductURL       string                   `json:"product_url"`
	ProductPrice     []*benchmarkProductPrice `json:"product_price"`
	HasPayPal        bool                     `json:"has_paypal,omitempty"`
}

type benchmarkPtoClass struct {
	ClassCode        string `json:"class_code"`
	GoodsCode        string `json:"goods_code"`
	GoodsDescription string `json:"goods_description"`
}

type benchmarkProduct struct {
	UID                 string                     `json:"uid"`
	UIDs                []string                   `json:"uids"`
	Platform            string                     `json:"platform"`
	Country             string                     `json:"country,omitempty"`
	ID                  string                     `json:"id"`
	URL                 string                     `json:"url"`
	Title               string                     `json:"title,omitempty"`
	Description         string                     `json:"description,omitempty"`
	Condition           string                     `json:"condition,omitempty"`
	Category            []string                   `json:"category,omitempty"`
	Gallery             []*benchmarkImage          `json:"gallery,omitempty"`
	Brand               string                     `json:"brand,omitempty"`
	Solds               []*benchmarkProductSold    `json:"solds,omitempty"`
	Prices              []*benchmarkProductPrice   `json:"prices,omitempty"`
	Stocks              []*benchmarkProductStock   `json:"stocks,omitempty"`
	CommentCount        int64                      `json:"comment_count,omitempty"`
	Comments            []*benchmarkProductComment `json:"comments,omitempty"`
	Rating              *benchmarkRating           `json:"rating,omitempty"`
	Offers              []*benchmarkProductOffer   `json:"offers,omitempty"`
	AllowedCountries    []string                   `json:"allowed_countries,omitempty"`
	RestrictedCountries []string                   `json:"restricted_countries,omitempty"`
	SerialNumber        string                     `json:"serial_number,omitempty"`
	Languages           []string                   `json:"languages,omitempty"`
	CountriesFromIP     []string                   `json:"countries_from_ip,omitempty"`
	SoldByPlatform      bool                       `json:"sold_by_platform,omitempty"`
	FromMaybe           bool                       `json:"from_maybe,omitempty"`
	Available           bool                       `json:"available,omitempty"`
	FirstFoundAt        *time.Time                 `json:"first_found_at,omitempty"`
	LastFoundAt         *time.Time                 `json:"last_found_at,omitempty"`
	EvictedAt           *time.Time                 `json:"evicted_at,omitempty"`
	Hostnames           []string                   `json:"hostnames,omitempty"`
	EcommerceClass      string                     `json:"ecommerce_class,omitempty"`
	PtoClass            []*benchmarkPtoClass       `json:"pto_class,omitempty"`
	Brands              []string                   `json:"brands,omitempty"`
	TranslatedText      string                     `json:"translated_text,omitempty"`
}

func newBenchmarkProduct() *benchmarkProduct {
	product := &benchmarkProduct{}
	return product
}

func (p *benchmarkProduct) Merge(incoming *benchmarkProduct) {
	p.UIDs = append(p.UIDs, p.UID)
	if incoming.UID != "" {
		p.UIDs = append(p.UIDs, incoming.UID)
		p.UID = incoming.UID
	}
	p.UIDs = append(p.UIDs, incoming.UIDs...)
	p.UIDs = deduplicateStrings(p.UIDs, true)

	replaceString(&p.Platform, incoming.Platform)
	replaceString(&p.Country, incoming.Country)
	replaceString(&p.ID, incoming.ID)
	replaceString(&p.URL, incoming.URL)
	replaceString(&p.Title, incoming.Title)
	replaceString(&p.Description, incoming.Description)
	replaceString(&p.Condition, incoming.Condition)
	replaceSlice(&p.Category, incoming.Category)
	replaceSlice(&p.Gallery, incoming.Gallery)
	if incoming.Brand != "" {
		p.Brand = strings.ToUpper(incoming.Brand)
	}

	if len(incoming.Solds) > 0 {
		p.Solds = append(p.Solds, incoming.Solds...)
		p.Solds = deduplicateBy(p.Solds, productSoldKey)
	}
	p.Solds = keepTail(p.Solds, 20)
	replaceSlice(&p.Prices, incoming.Prices)
	if len(incoming.Stocks) > 0 {
		p.Stocks = append(p.Stocks, incoming.Stocks...)
		p.Stocks = deduplicateBy(p.Stocks, productStockKey)
	}
	p.Stocks = keepTail(p.Stocks, 20)

	if incoming.CommentCount != 0 {
		p.CommentCount = incoming.CommentCount
	}
	if len(incoming.Comments) > 0 {
		p.Comments = append(p.Comments, incoming.Comments...)
		p.Comments = deduplicateBy(p.Comments, jsonDigest[*benchmarkProductComment])
	}
	p.Comments = keepTail(p.Comments, 20)
	if incoming.Rating != nil {
		p.Rating = incoming.Rating
	}

	if len(incoming.Offers) > 0 {
		offers := append([]*benchmarkProductOffer(nil), incoming.Offers...)
		offers = append(offers, p.Offers...)
		p.Offers = deduplicateBy(offers, func(offer *benchmarkProductOffer) string {
			return offer.UID
		})
	}
	p.Offers = keepTail(p.Offers, 50)

	if len(incoming.AllowedCountries) > 0 {
		p.AllowedCountries = append(p.AllowedCountries, incoming.AllowedCountries...)
		p.AllowedCountries = deduplicateStrings(p.AllowedCountries, false)
	}
	if len(incoming.RestrictedCountries) > 0 {
		p.RestrictedCountries = append(p.RestrictedCountries, incoming.RestrictedCountries...)
		p.RestrictedCountries = append(p.RestrictedCountries, incoming.RestrictedCountries...)
		p.RestrictedCountries = deduplicateStrings(p.RestrictedCountries, false)
	}

	if incoming.LastFoundAt != nil {
		p.LastFoundAt = incoming.LastFoundAt
	}
	if p.FirstFoundAt == nil && incoming.FirstFoundAt != nil {
		p.FirstFoundAt = incoming.FirstFoundAt
	}
	if incoming.EvictedAt != nil {
		p.EvictedAt = incoming.EvictedAt
	}

	replaceSlice(&p.Hostnames, incoming.Hostnames)
	replaceString(&p.EcommerceClass, incoming.EcommerceClass)
	replaceSlice(&p.PtoClass, incoming.PtoClass)
	replaceSlice(&p.Brands, incoming.Brands)
	replaceString(&p.TranslatedText, incoming.TranslatedText)
	if len(incoming.Languages) > 0 {
		p.Languages = append(p.Languages, incoming.Languages...)
		p.Languages = deduplicateStrings(p.Languages, false)
	}
	if len(incoming.CountriesFromIP) > 0 {
		p.CountriesFromIP = append(p.CountriesFromIP, incoming.CountriesFromIP...)
		p.CountriesFromIP = deduplicateStrings(p.CountriesFromIP, false)
	}
	replaceString(&p.SerialNumber, incoming.SerialNumber)
	p.SoldByPlatform = incoming.SoldByPlatform
	p.Available = incoming.Available
}

func replaceString(target *string, incoming string) {
	if incoming != "" {
		*target = incoming
	}
}

func replaceSlice[T any](target *[]T, incoming []T) {
	if len(incoming) > 0 {
		*target = incoming
	}
}

func deduplicateStrings(items []string, removeEmpty bool) []string {
	result := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if removeEmpty && item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

func deduplicateBy[T any, K comparable](items []T, key func(T) K) []T {
	result := make([]T, 0, len(items))
	seen := make(map[K]struct{}, len(items))
	for _, item := range items {
		itemKey := key(item)
		if _, exists := seen[itemKey]; exists {
			continue
		}
		seen[itemKey] = struct{}{}
		result = append(result, item)
	}
	return result
}

func keepTail[T any](items []T, limit int) []T {
	if len(items) <= limit {
		return items
	}
	return items[len(items)-limit:]
}

type soldKey struct {
	sold        int64
	periodHours int64
	recordDate  string
}

func productSoldKey(item *benchmarkProductSold) soldKey {
	recordDate := ""
	if item.RecordAt != nil {
		recordDate = item.RecordAt.Format("2006-01-02")
	}
	key := soldKey{sold: item.Sold, periodHours: item.PeriodHours, recordDate: recordDate}
	return key
}

type stockKey struct {
	stock     int64
	variables [sha1.Size]byte
	hasValues bool
}

func productStockKey(item *benchmarkProductStock) stockKey {
	key := stockKey{stock: item.Stock}
	if len(item.Variables) > 0 {
		key.variables = jsonDigest(item.Variables)
		key.hasValues = true
	}
	return key
}

func jsonDigest[T any](value T) [sha1.Size]byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal benchmark hash input: %v", err))
	}
	return sha1.Sum(encoded)
}
