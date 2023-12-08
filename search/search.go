package search

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var ErrInvalidSearchSyntax = errors.New("invalid search syntax")

type SearchSymbol string

var (
	SearchSymbolNone   SearchSymbol = ""
	SearchSymbolRange  SearchSymbol = ".."
	SearchSymbolNot    SearchSymbol = "!="
	SearchSymbolEq     SearchSymbol = "="
	SearchSymbolGt     SearchSymbol = ">"
	SearchSymbolGte    SearchSymbol = ">="
	SearchSymbolLt     SearchSymbol = "<"
	SearchSymbolLte    SearchSymbol = "<="
	SearchSymbolSearch SearchSymbol = "search"
)

type SearchValue struct {
	Symbol SearchSymbol
	Value  any
	Value2 any
}

type SearchData map[string][]SearchValue

type Order struct {
	Name string
	Desc bool
}

type SearchDataHandleFunc func(values []SearchValue) (string, []any)
type SearchDataHandleFuncMap map[string]SearchDataHandleFunc

func ParseOrderByString(text string) []Order {
	orders := []Order{}
	parts := strings.Split(text, ",")
	for _, name := range parts {
		desc := false
		if strings.HasSuffix(name, "-") {
			desc = true
			name = name[:len(name)-1]
		} else if strings.HasSuffix(name, "+") {
			desc = false
			name = name[:len(name)-1]
		}
		if name == "" {
			continue
		}
		orders = append(orders, Order{Name: name, Desc: desc})
	}
	return orders
}

func ParseSearchString(text string) (SearchData, error) {
	search, err := ParseSearchString2(text)
	if err != nil {
		return search, err
	}
	search2 := SearchData{}
	subSearchs := map[string]SearchData{}
	for name, values := range search {
		parts := strings.Split(name, ".")
		if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
			sub, ok := subSearchs[parts[0]]
			if !ok {
				sub = SearchData{}
			}
			sub[parts[1]] = values
			subSearchs[parts[0]] = sub
		} else {
			search2[name] = values
		}
	}
	for name, sub := range subSearchs {
		search2[name] = []SearchValue{
			{
				Symbol: SearchSymbolSearch,
				Value:  sub,
			},
		}
	}
	search = search2
	return search, nil
}

func (v SearchValue) String() string {
	if v.Symbol == SearchSymbolRange {
		return fmt.Sprintf("%v:%v", v.Value, v.Value2)
	} else {
		symbol := ""
		switch v.Symbol {
		case SearchSymbolNone:
		case SearchSymbolNot:
			symbol = "!="
		case SearchSymbolEq:
		case SearchSymbolGt:
			symbol = ">"
		case SearchSymbolGte:
			symbol = ">="
		case SearchSymbolLt:
			symbol = "<"
		case SearchSymbolLte:
			symbol = "<="
		}
		if vv, ok := v.Value.(string); ok {
			return fmt.Sprintf("%s'%s'", symbol, vv)
		} else {
			return fmt.Sprintf("%s%v", symbol, v.Value)
		}
	}
}

func (s SearchData) String() string {
	parts := []string{}
	for key, values := range s {
		vparts := []string{}
		for _, v := range values {
			if v.Symbol == SearchSymbolSearch {
				if vv, ok := v.Value.(SearchData); ok {
					for subKey, subValues := range vv {
						for _, subValue := range subValues {
							parts = append(parts, fmt.Sprintf("%s.%s:%s", key, subKey, subValue.String()))
						}
					}
				}
			} else {
				vparts = append(vparts, v.String())
			}
		}
		if len(vparts) > 0 {
			parts = append(parts, fmt.Sprintf("%s:%s", key, strings.Join(vparts, ",")))
		}
	}
	return strings.Join(parts, " ")
}

func splitSearchString(text string) []string {
	var start int
	var parts []string
	text = strings.TrimSpace(text)
	if text == "" {
		return parts
	}

	for {
		spaceIdx := strings.Index(text[start:], " ")
		quoteIdx := strings.Index(text[start:], "'")

		if spaceIdx == -1 && quoteIdx == -1 {
			parts = append(parts, strings.TrimSpace(text[start:]))
			break
		}

		if spaceIdx != -1 && (quoteIdx == -1 || spaceIdx < quoteIdx) {
			parts = append(parts, strings.TrimSpace(text[start:start+spaceIdx]))
			start += spaceIdx + 1
		} else {
			nextQuoteIdx := strings.Index(text[start+quoteIdx+1:], "'")
			if nextQuoteIdx == -1 {
				break
			}
			parts = append(parts, strings.TrimSpace(text[start:start+quoteIdx+nextQuoteIdx+2]))
			start += quoteIdx + nextQuoteIdx + 2
		}
	}
	return parts
}

func ParseSearchString2(text string) (SearchData, error) {
	pattern1 := "^((?:[$]{0,1})[A-Za-z0-9_\\-\\p{Han}\\.]+)$"
	re1 := regexp.MustCompile(pattern1)
	pattern2 := "^(?:((?:[$]{0,1})[a-zA-Z0-9_\\-\\.]+):((?:(?:>|>=|<|<=|!=){0,1}[\"']{0,1}(?:(?:[$]{0,1})[a-zA-Z0-9\\p{Han}_\\.\\-:\\+\\*]*)[\"']{0,1})(?:,(?:(?:>|>=|<|<=|!=){0,1}[\"']{0,1}(?:(?:[$]{0,1})[a-zA-Z0-9\u4e00-\u9fa5_\\.\\-:\\+\\*]*)[\"']{0,1}))*))$"
	re2 := regexp.MustCompile(pattern2)
	pattern3 := "^(?:(>|>=|<|<=|!=){0,1}((?:[$]{0,1})[a-zA-Z0-9\\p{Han}_\\.\\-:\\+\\*\"']*))$"
	re3 := regexp.MustCompile(pattern3)
	pattern4 := "^(?:((?:[$]{0,1})[a-zA-Z0-9_\\-\\.]+):[\"'](.*)[\"'])$"
	re4 := regexp.MustCompile(pattern4)

	strPattern := "^(?:[\"'](.*)[\"'])$"
	strRe := regexp.MustCompile(strPattern)
	intRangePattern := `^(?:(?:(\d+|\*)\.\.(\d+|\*))|(\d+))$`
	intRangeRe := regexp.MustCompile(intRangePattern)
	floatRangePattern := `^(?:(?:([1-9]\d*\.\d*|0\.\d*[1-9]\d*|\d+|\*)\.\.([1-9]\d*\.\d*|0\.\d*[1-9]\d*|\d+|\*))|([1-9]\d*\.\d*|0\.\d*[1-9]\d*|\d+))$`
	floatRangeRe := regexp.MustCompile(floatRangePattern)
	timeRangePattern := `^(?:((?:(?:\d{4}\-\d{2}\-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:(?:[+-]{1}\d{2}:\d{2}){0,1}|Z){0,1})))|\*)\.\.((?:(?:\d{4}\-\d{2}\-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:(?:[+-]{1}\d{2}:\d{2}){0,1}|Z){0,1})))|\*)|^((?:\d{4}\-\d{2}\-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:(?:[+-]{1}\d{2}:\d{2}){0,1}|Z){0,1}))))$`
	timeRangeRe := regexp.MustCompile(timeRangePattern)

	parts := splitSearchString(text)
	search := SearchData{}
	// utils.GetLogger().Warn("debug", zap.String("text", text), zap.Strings("parts", parts))

	for _, part := range parts {
		if part == "" {
			continue
		}
		if subs := re2.FindStringSubmatch(part); len(subs) == 3 {
			var err error
			name := subs[1]
			rawValues := strings.Split(subs[2], ",")
			values := make([]SearchValue, 0)

			for _, rawValue := range rawValues {
				subs2 := re3.FindStringSubmatch(rawValue)
				if len(subs2) != 3 {
					return search, errors.Wrap(ErrInvalidSearchSyntax, "unexpected format "+rawValue)
				}
				value := SearchValue{
					Symbol: SearchSymbolNone,
					Value:  nil,
					Value2: nil,
				}
				switch subs2[1] {
				case "":
					value.Symbol = SearchSymbolNone
				case "!=":
					value.Symbol = SearchSymbolNot
				case ">":
					value.Symbol = SearchSymbolGt
				case ">=":
					value.Symbol = SearchSymbolGte
				case "<":
					value.Symbol = SearchSymbolLt
				case "<=":
					value.Symbol = SearchSymbolLte
				default:
					return search, errors.Wrap(ErrInvalidSearchSyntax, "unexpected symbol, "+rawValue)
				}

				if subs3 := strRe.FindStringSubmatch(subs2[2]); len(subs3) == 2 {
					if value.Symbol == SearchSymbolNone {
						value.Symbol = SearchSymbolEq
					}
					value.Value = subs3[1]
				} else if subs3 := intRangeRe.FindStringSubmatch(subs2[2]); len(subs3) == 4 {
					if subs3[1] != "" || subs3[2] != "" {
						if value.Symbol != SearchSymbolNone {
							return search, errors.Wrap(err, "parse int range error, "+rawValue)
						}
						value.Symbol = SearchSymbolRange

						if subs3[1] != "*" {
							value.Value, err = strconv.ParseInt(subs3[1], 10, 64)
							if err != nil {
								return search, errors.Wrap(err, "parse int error, "+rawValue)
							}
						}
						if subs3[2] != "*" {
							value.Value2, err = strconv.ParseInt(subs3[2], 10, 64)
							if err != nil {
								return search, errors.Wrap(err, "parse int error, "+rawValue)
							}
						}
					} else if subs3[3] != "" {
						if value.Symbol == SearchSymbolNone {
							value.Symbol = SearchSymbolEq
						}
						value.Value, err = strconv.ParseInt(subs3[3], 10, 64)
						if err != nil {
							return search, errors.Wrap(ErrInvalidSearchSyntax, "parse int error, "+rawValue)
						}
					} else {
						return search, errors.Wrap(ErrInvalidSearchSyntax, "parse int range error, "+rawValue)
					}
				} else if subs3 := floatRangeRe.FindStringSubmatch(subs2[2]); len(subs3) == 4 {
					if subs3[1] != "" || subs3[2] != "" {
						if value.Symbol != SearchSymbolNone {
							return search, errors.Wrap(ErrInvalidSearchSyntax, "parse float range error, "+rawValue)
						}
						value.Symbol = SearchSymbolRange

						if subs3[1] != "*" {
							value.Value, err = strconv.ParseFloat(subs3[1], 64)
							if err != nil {
								return search, errors.Wrap(err, "parse float error, "+rawValue)
							}
						}
						if subs3[2] != "*" {
							value.Value2, err = strconv.ParseFloat(subs3[2], 64)
							if err != nil {
								return search, errors.Wrap(err, "parse float error, "+rawValue)
							}
						}
					} else if subs3[3] != "" {
						if value.Symbol == SearchSymbolNone {
							value.Symbol = SearchSymbolEq
						}
						value.Value, err = strconv.ParseFloat(subs3[3], 64)
						if err != nil {
							return search, errors.Wrap(err, "parse float error, "+rawValue)
						}
					} else {
						return search, errors.Wrap(ErrInvalidSearchSyntax, "parse float range error, "+rawValue)
					}
				} else if subs3 := timeRangeRe.FindStringSubmatch(subs2[2]); len(subs3) == 4 {
					if subs3[1] != "" || subs3[2] != "" {
						if value.Symbol != SearchSymbolNone {
							return search, errors.Wrap(ErrInvalidSearchSyntax, "parse time range error, "+rawValue)
						}
						value.Symbol = SearchSymbolRange

						if subs3[1] != "*" {
							value.Value, err = time.Parse(time.RFC3339, subs3[1])
							if err != nil {
								return search, errors.Wrap(err, "parse time error, "+rawValue)
							}
						}
						if subs3[2] != "*" {
							value.Value2, err = time.Parse(time.RFC3339, subs3[2])
							if err != nil {
								return search, errors.Wrap(err, "parse time error, "+rawValue)
							}
						}
					} else if subs3[3] != "" {
						if value.Symbol == SearchSymbolNone {
							value.Symbol = SearchSymbolEq
						}
						value.Value, err = time.Parse(time.RFC3339, subs3[3])
						if err != nil {
							return search, errors.Wrap(err, "parse time error, "+rawValue)
						}
					} else {
						return search, errors.Wrap(ErrInvalidSearchSyntax, "parse time range error, "+rawValue)
					}
				} else {
					if value.Symbol == SearchSymbolNone {
						value.Symbol = SearchSymbolEq
					}
					value.Value = subs2[2]
				}
				values = append(values, value)
			}
			if len(values) > 0 {
				search[name] = values
			}
		} else if re1.MatchString(part) {
			search[part] = nil
		} else if subs := re4.FindStringSubmatch(part); len(subs) == 3 {
			values := make([]SearchValue, 0)
			value := SearchValue{
				Symbol: SearchSymbolEq,
				Value:  subs[2],
				Value2: nil,
			}
			values = append(values, value)
			search[subs[1]] = values
		} else {
			return search, errors.Wrap(ErrInvalidSearchSyntax, part)
		}
	}

	return search, nil
}

func (m SearchData) SearchDB(db *gorm.DB, handleFuncs map[string]SearchDataHandleFunc) *gorm.DB {
	for k, vals := range m {
		if handleFuncs != nil {
			if f, ok := handleFuncs[k]; ok && f != nil {
				if query2, args2 := f(vals); query2 != "" {
					db = db.Where(query2, args2)
				}
				continue
			}
		}
		var subConditions []string
		var subArgs []interface{}
		for _, val := range vals {
			switch val.Symbol {
			case SearchSymbolNone:
				// do nothing
			case SearchSymbolRange:
				subConditions = append(subConditions, k+" BETWEEN ? AND ?")
				subArgs = append(subArgs, val.Value, val.Value2)
			case SearchSymbolNot:
				subConditions = append(subConditions, k+" != ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolEq:
				subConditions = append(subConditions, k+" = ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolGt:
				subConditions = append(subConditions, k+" > ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolGte:
				subConditions = append(subConditions, k+" >= ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolLt:
				subConditions = append(subConditions, k+" < ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolLte:
				subConditions = append(subConditions, k+" <= ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolSearch:
				if sub, ok := val.Value.(SearchData); ok {
					sub2 := SearchData{}
					for subName, subValues := range sub {
						if len(subValues) > 0 {
							for subIndex, subValue := range subValues {
								newSubValue := SearchValue{
									Symbol: subValue.Symbol,
								}
								if subValue.Value != nil {
									newSubValue.Value = fmt.Sprintf("%v", subValue.Value)
								}
								if subValue.Value2 != nil {
									newSubValue.Value2 = fmt.Sprintf("%v", subValue.Value2)
								}
								subValues[subIndex] = newSubValue
							}
							sub2[fmt.Sprintf("%s->>'%s'", k, subName)] = subValues
						}
					}
					db = sub2.SearchDB(db, handleFuncs)
				}
			}
		}
		if len(subConditions) > 0 {
			subQuery := strings.Join(subConditions, " OR ")
			db = db.Where(subQuery, subArgs...)
		}
	}
	return db
}

func (m SearchData) WhereString(handleFuncs SearchDataHandleFuncMap) (string, []any, error) {
	var queries []string
	var args []any
	for k, vals := range m {
		if handleFuncs != nil {
			if f, ok := handleFuncs[k]; ok && f != nil {
				if query2, args2 := f(vals); query2 != "" {
					queries = append(queries, query2)
					if args2 != nil {
						args = append(args, args2...)
					}
				}
				continue
			}
		}
		var subConditions []string
		var subArgs []any
		for _, val := range vals {
			switch val.Symbol {
			case SearchSymbolNone:
				// do nothing
			case SearchSymbolRange:
				subConditions = append(subConditions, k+" BETWEEN ? AND ?")
				subArgs = append(subArgs, val.Value, val.Value2)
			case SearchSymbolNot:
				subConditions = append(subConditions, k+" != ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolEq:
				subConditions = append(subConditions, k+" = ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolGt:
				subConditions = append(subConditions, k+" > ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolGte:
				subConditions = append(subConditions, k+" >= ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolLt:
				subConditions = append(subConditions, k+" < ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolLte:
				subConditions = append(subConditions, k+" <= ?")
				subArgs = append(subArgs, val.Value)
			case SearchSymbolSearch:
				if sub, ok := val.Value.(SearchData); ok {
					sub2 := SearchData{}
					for subName, subValues := range sub {
						if len(subValues) > 0 {
							for subIndex, subValue := range subValues {
								newSubValue := SearchValue{
									Symbol: subValue.Symbol,
								}
								if subValue.Value != nil {
									newSubValue.Value = fmt.Sprintf("%v", subValue.Value)
								}
								if subValue.Value2 != nil {
									newSubValue.Value2 = fmt.Sprintf("%v", subValue.Value2)
								}
								subValues[subIndex] = newSubValue
							}
							sub2[fmt.Sprintf("%s->>'%s'", k, subName)] = subValues
						}
					}
					sub2Query, sub2Args, err := sub2.WhereString(handleFuncs)
					if err != nil {
						return "", nil, err
					}
					queries = append(queries, sub2Query)
					args = append(args, sub2Args...)
				}
			}
		}
		if len(subConditions) > 0 {
			queries = append(queries, "("+strings.Join(subConditions, " OR ")+")")
			args = append(args, subArgs...)
		}
	}
	if len(queries) == 0 {
		return "", nil, errors.New("no valid search conditions")
	}
	return strings.Join(queries, " AND "), args, nil
}
