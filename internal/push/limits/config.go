package limits

// Config WS 连接与订阅上限（对齐 rest-api §8.2）。
type Config struct {
	RetailMaxConnections          int `json:"retail_max_connections"`
	RetailMaxSymbolsPerConnection int `json:"retail_max_symbols_per_connection"`
	MarketMakerMaxConnections     int `json:"market_maker_max_connections"`
}

// WithDefaults 填充默认值。
func (c Config) WithDefaults() Config {
	if c.RetailMaxConnections <= 0 {
		c.RetailMaxConnections = 5
	}
	if c.RetailMaxSymbolsPerConnection <= 0 {
		c.RetailMaxSymbolsPerConnection = 50
	}
	if c.MarketMakerMaxConnections <= 0 {
		c.MarketMakerMaxConnections = 3
	}
	return c
}

// MaxConnections 返回 subject 允许的最大并发 WS 连接数。
func (c Config) MaxConnections(marketMaker bool) int {
	if marketMaker {
		return c.MarketMakerMaxConnections
	}
	return c.RetailMaxConnections
}
