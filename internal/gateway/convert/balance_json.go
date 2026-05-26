package convert

import orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"

// ListBalancesData GET /v1/balances 响应 data。
type ListBalancesData struct {
	Items []BalanceJson `json:"items"`
}

// GetBalanceData GET /v1/balances/{asset} 响应 data。
type BalanceJson struct {
	Asset     string `json:"asset"`
	Balance   string `json:"balance"`
	Frozen    string `json:"frozen"`
	Available string `json:"available"`
}

func ListBalancesResponseFromPB(resp *orderv1.ListBalancesResponse) ListBalancesData {
	if resp == nil {
		return ListBalancesData{}
	}

	items := make([]BalanceJson, 0, len(resp.GetBalances()))
	for _, balance := range resp.GetBalances() {
		items = append(items, GetBalanceResponseFromPB(balance))
	}
	return ListBalancesData{
		Items: items,
	}
}

func GetBalanceResponseFromPB(resp *orderv1.Balance) BalanceJson {
	if resp == nil {
		return BalanceJson{}
	}
	return BalanceJson{
		Asset:     resp.GetAsset(),
		Balance:   DecimalToString(resp.GetBalance()),
		Frozen:    DecimalToString(resp.GetFrozen()),
		Available: DecimalToString(resp.GetAvailable()),
	}
}
