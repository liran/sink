package luabench

import (
	"encoding/json"
	"fmt"
	"time"
)

type benchmarkFixture struct {
	name         string
	currentJSON  []byte
	incomingJSON []byte
}

type fixtureOptions struct {
	name      string
	itemCount int
}

func benchmarkFixtures() []benchmarkFixture {
	options := []fixtureOptions{
		{name: "small", itemCount: 2},
		{name: "medium", itemCount: 50},
		{name: "large", itemCount: 250},
	}
	fixtures := make([]benchmarkFixture, 0, len(options))
	for _, option := range options {
		fixtures = append(fixtures, makeBenchmarkFixture(option))
	}
	return fixtures
}

func makeBenchmarkFixture(options fixtureOptions) benchmarkFixture {
	current := makeProduct("current", 0, options.itemCount)
	incomingCount := options.itemCount / 2
	if incomingCount < 1 {
		incomingCount = 1
	}
	incoming := makeProduct("incoming", options.itemCount/2, incomingCount)
	incoming.UID = current.UID
	incoming.ID = current.ID
	incoming.URL = current.URL
	incoming.Platform = current.Platform
	incoming.Country = current.Country
	incoming.Brand = "incoming brand"
	incoming.AllowedCountries = []string{"US", "JP", "DE"}
	incoming.RestrictedCountries = []string{"CN", "RU"}
	incoming.Languages = []string{"en", "ja", "de"}
	incoming.CountriesFromIP = []string{"US", "JP"}
	incoming.SoldByPlatform = true
	incoming.Available = true

	return marshalBenchmarkFixture(options.name, current, incoming)
}

func makeSparseBenchmarkFixture() benchmarkFixture {
	current := makeProduct("current", 0, 2)
	current.UIDs = []string{"legacy:benchmark-product", "", "legacy:benchmark-product"}
	current.Solds = nil
	current.Stocks = nil
	current.Comments = nil
	current.Offers = nil
	current.AllowedCountries = []string{"US", "US"}
	current.RestrictedCountries = []string{"CN", "CN"}
	current.Languages = []string{"en", "en"}
	current.CountriesFromIP = []string{"US", "US"}

	incoming := newBenchmarkProduct()
	incoming.UID = current.UID
	return marshalBenchmarkFixture("sparse-incoming", current, incoming)
}

func makeTrimOnlyBenchmarkFixture() benchmarkFixture {
	current := makeProduct("current", 0, 75)
	incoming := newBenchmarkProduct()
	incoming.UID = current.UID
	return marshalBenchmarkFixture("trim-without-incoming-history", current, incoming)
}

func makeBalancedBenchmarkFixture(name string, itemCount int) benchmarkFixture {
	current := makeProduct("current", 0, itemCount)
	incoming := makeProduct("incoming", itemCount, itemCount)
	incoming.UID = current.UID
	incoming.ID = current.ID
	incoming.URL = current.URL
	incoming.Platform = current.Platform
	incoming.Country = current.Country
	return marshalBenchmarkFixture(name, current, incoming)
}

func marshalBenchmarkFixture(name string, current, incoming *benchmarkProduct) benchmarkFixture {
	currentJSON, err := json.Marshal(current)
	if err != nil {
		panic(err)
	}
	incomingJSON, err := json.Marshal(incoming)
	if err != nil {
		panic(err)
	}
	fixture := benchmarkFixture{
		name:         name,
		currentJSON:  currentJSON,
		incomingJSON: incomingJSON,
	}
	return fixture
}

func makeProduct(prefix string, offset, itemCount int) *benchmarkProduct {
	product := newBenchmarkProduct()
	product.UID = "generic:benchmark-product"
	product.UIDs = []string{"legacy:benchmark-product"}
	product.Platform = "generic"
	product.Country = "US"
	product.ID = "benchmark-product"
	product.URL = "https://example.com/products/benchmark-product"
	product.Title = prefix + " benchmark product"
	product.Description = repeatedText(prefix, 12)
	product.Condition = "new"
	product.Category = []string{"apparel", "shoes", prefix}
	product.Brand = prefix + " brand"
	product.CommentCount = int64(offset + itemCount)
	product.Rating = &benchmarkRating{Score: 4.5, Count: int64(offset + itemCount)}
	product.AllowedCountries = []string{"US", "GB"}
	product.RestrictedCountries = []string{"CN"}
	product.Languages = []string{"en", "fr"}
	product.CountriesFromIP = []string{"US"}
	product.SerialNumber = prefix + "-serial"
	product.Available = true
	product.Hostnames = []string{"example.com", prefix + ".example.com"}
	product.EcommerceClass = "retail"
	product.PtoClass = []*benchmarkPtoClass{
		{
			ClassCode:        "025",
			GoodsCode:        "025-01",
			GoodsDescription: "clothing",
		},
	}
	product.Brands = []string{prefix + " brand", "shared brand"}
	product.TranslatedText = repeatedText(prefix+" translated", 4)

	firstFoundAt := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	lastFoundAt := firstFoundAt.Add(time.Hour)
	product.FirstFoundAt = &firstFoundAt
	product.LastFoundAt = &lastFoundAt

	galleryCount := itemCount
	if galleryCount > 20 {
		galleryCount = 20
	}
	priceCount := itemCount
	if priceCount > 10 {
		priceCount = 10
	}
	for index := 0; index < galleryCount; index++ {
		position := offset + index
		image := &benchmarkImage{
			URL: fmt.Sprintf("https://images.example.com/%s/%d.jpg", prefix, position),
			Key: fmt.Sprintf("%s/images/%d.jpg", prefix, position),
		}
		product.Gallery = append(product.Gallery, image)
	}
	for index := 0; index < priceCount; index++ {
		position := offset + index
		date := firstFoundAt.AddDate(0, 0, position)
		price := &benchmarkProductPrice{
			Currency: "USD",
			Price:    float64(position) + 19.99,
			Dollar:   float64(position) + 19.99,
			Variables: map[string]any{
				"color": fmt.Sprintf("color-%d", position%8),
				"size":  fmt.Sprintf("size-%d", position%12),
			},
			Date: &date,
		}
		product.Prices = append(product.Prices, price)
	}

	for index := 0; index < itemCount; index++ {
		position := offset + index
		appendHistoryItem(product, prefix, position, firstFoundAt)
	}
	return product
}

func appendHistoryItem(product *benchmarkProduct, prefix string, position int, baseTime time.Time) {
	recordAt := baseTime.AddDate(0, 0, position)
	sold := &benchmarkProductSold{
		Sold:        int64(position + 1),
		PeriodHours: 24,
		RecordAt:    &recordAt,
	}
	product.Solds = append(product.Solds, sold)

	stock := &benchmarkProductStock{
		Stock: int64(position % 41),
		Variables: map[string]any{
			"color": fmt.Sprintf("color-%d", position%8),
			"size":  fmt.Sprintf("size-%d", position%12),
		},
	}
	product.Stocks = append(product.Stocks, stock)

	commentDate := baseTime.Add(time.Duration(position) * time.Minute)
	comment := &benchmarkProductComment{
		Score:           float64(position%5) + 1,
		Title:           fmt.Sprintf("%s comment %d", prefix, position),
		Description:     repeatedText(fmt.Sprintf("comment-%d", position), 3),
		CustomerName:    fmt.Sprintf("customer-%d", position),
		CustomerProfile: fmt.Sprintf("https://example.com/customer/%d", position),
		Location:        "US",
		Currency:        "USD",
		Price:           float64(position) + 9.99,
		Date:            &commentDate,
	}
	product.Comments = append(product.Comments, comment)

	priceDate := baseTime.AddDate(0, 0, position)
	price := &benchmarkProductPrice{
		Currency: "USD",
		Price:    float64(position) + 29.99,
		Dollar:   float64(position) + 29.99,
		Variables: map[string]any{
			"color": fmt.Sprintf("color-%d", position%8),
			"size":  fmt.Sprintf("size-%d", position%12),
		},
		Date: &priceDate,
	}
	offer := &benchmarkProductOffer{
		ID:           fmt.Sprintf("offer-%d", position),
		UID:          fmt.Sprintf("generic:offer-%d", position),
		URL:          fmt.Sprintf("https://seller.example.com/offers/%d", position),
		Name:         fmt.Sprintf("seller-%d", position),
		ProductURL:   product.URL,
		ProductPrice: []*benchmarkProductPrice{price},
		HasPayPal:    position%2 == 0,
	}
	product.Offers = append(product.Offers, offer)
}

func repeatedText(value string, count int) string {
	result := ""
	for index := 0; index < count; index++ {
		if result != "" {
			result += " "
		}
		result += value
	}
	return result
}

func nativeProductMerge(currentJSON, incomingJSON []byte) ([]byte, error) {
	current := newBenchmarkProduct()
	if err := json.Unmarshal(currentJSON, current); err != nil {
		return nil, err
	}
	incoming := newBenchmarkProduct()
	if err := json.Unmarshal(incomingJSON, incoming); err != nil {
		return nil, err
	}
	current.Merge(incoming)
	return json.Marshal(current)
}
