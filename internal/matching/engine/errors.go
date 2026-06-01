package engine

import "errors"

var ErrSymbolRequired = errors.New("symbol required")

// ErrSymbolReadOnly 对账失败后该 symbol 拒收新单（§5.6）。
var ErrSymbolReadOnly = errors.New("symbol read-only")
