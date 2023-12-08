package tsdb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/heypkg/store/search"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

type TSDataFormat string

const (
	TSDataFormatTimeSerial = "time_serial"
)

type TSQuerySelect struct {
	Name   string `json:"Name"`
	Source string `json:"Source"`
	Func   string `json:"Func"`
}

type TSQuery struct {
	Id           string            `json:"Id"`
	Source       string            `json:"Source"`
	Interval     float64           `json:"Interval"`
	Format       TSDataFormat      `json:"Format"`
	Select       []TSQuerySelect   `json:"Select"`
	GroupBy      []string          `json:"GroupBy"`
	OrderBy      []string          `json:"OrderBy"`
	Reverse      bool              `json:"Reverse"`
	SearchString string            `json:"Search"`
	Search       search.SearchData `json:"-"`
	Limit        int               `json:"Limit"`
	Offset       int               `json:"Offset"`
}

type TSQueryCommand struct {
	Schema   string  `json:"-"`
	From     int64   `json:"From"`
	To       int64   `json:"To"`
	Query    TSQuery `json:"Query"`
	TimeZone string  `json:"TimeZone"`
}
type TSPointValue any
type TSPoint map[string]TSPointValue

type TSSeries struct {
	Points  []TSPoint         `json:"Points"`
	GroupBy map[string]string `json:"GroupBy,omitempty"`
}

type TSResult struct {
	From int64      `json:"From"`
	To   int64      `json:"To"`
	Data []TSSeries `json:"Data"`
}

func HandleTSQueryCommand(db *gorm.DB, cmd TSQueryCommand) (TSResult, error) {
	result := TSResult{
		From: cmd.From,
		To:   cmd.To,
		Data: make([]TSSeries, 0),
	}
	query := cmd.Query

	searchString := fmt.Sprintf(`%v schema:"%v"`, query.SearchString, cmd.Schema)
	search, err := search.ParseSearchString2(searchString)
	if err != nil {
		return result, err
	}
	query.Search = search
	series, err := QueryTimeSeries(db, cmd.From, cmd.To, cmd.TimeZone, query)
	if err != nil {
		return result, err
	}
	result.Data = series
	return result, nil
}

func QueryTimeSeries(db *gorm.DB, from int64, to int64, tz string, query TSQuery) ([]TSSeries, error) {
	if query.Source == "" {
		return nil, errors.New("source is empty")
	}

	// _, err := time.LoadLocation(tz)
	// if err != nil {
	// 	return nil, err
	// }
	// Determine the bucket function to use based on the interval specified in the query.
	bucketFunc := "time"
	if query.Interval > 0 {
		bucketFunc = fmt.Sprintf("time_bucket('%f second', time, '%s')", query.Interval, tz)
	} else {
		bucketFunc = "time"
	}

	// Construct the SELECT clause of the SQL query based on the Select fields in the query.
	var selectClause []string
	var groupByClause []string
	var orderByClause []string
	var searchString string
	var searchArgs []any
	var offsetClause []string
	var limitClause []string
	var whereClause []string

	for _, sel := range query.Select {
		var expr string
		switch sel.Func {
		case "":
			expr = sel.Source
		case "text":
			expr = fmt.Sprintf("CAST(%s AS TEXT)", sel.Source)
		case "integer":
			expr = fmt.Sprintf("CAST(%s AS INTEGER)", sel.Source)
		case "float":
			expr = fmt.Sprintf("CAST(%s AS DOUBLE PRECISION)", sel.Source)
		case "avg":
			expr = fmt.Sprintf("AVG(CAST(%s AS DOUBLE PRECISION))", sel.Source)
		case "max":
			expr = fmt.Sprintf("MAX(%s)", sel.Source)
		case "min":
			expr = fmt.Sprintf("MIN(%s)", sel.Source)
		case "count":
			expr = fmt.Sprintf("COUNT(%s)", sel.Source)
		case "sum":
			expr = fmt.Sprintf("SUM(CAST(%s AS DOUBLE PRECISION))", sel.Source)
		case "first":
			expr = fmt.Sprintf("public.first(%s, time AT TIME ZONE '%s')", sel.Source, tz)
		case "last":
			expr = fmt.Sprintf("public.last(%s, time AT TIME ZONE '%s')", sel.Source, tz)
		case "delta":
			expr = fmt.Sprintf("%s - LAG(%s) OVER (ORDER BY time)", sel.Source, sel.Source)
		case "increase":
			expr = fmt.Sprintf(
				"(CASE WHEN %s >= LAG(%s) OVER (ORDER BY time) THEN %s - LAG(%s) OVER (ORDER BY time) ELSE NULL END)",
				sel.Source, sel.Source, sel.Source, sel.Source)
		case "rate":
			expr = fmt.Sprintf(
				"(CASE WHEN %s >= LAG(%s) OVER (ORDER BY time) THEN (%s - LAG(%s) OVER (ORDER BY time)) / EXTRACT(EPOCH FROM time - LAG(t) OVER (ORDER BY time)) ELSE NULL END)",
				sel.Source, sel.Source, sel.Source, sel.Source)
		default:
			return nil, errors.Errorf("unknown function: %s", sel.Func)
		}
		if sel.Name != "" {
			expr = expr + " AS " + sel.Name
		}
		selectClause = append(selectClause, expr)
	}
	if len(query.GroupBy) > 0 {
		groupByClause = append(groupByClause, query.GroupBy...)
	}
	if len(query.OrderBy) > 0 {
		orderByClause = append(orderByClause, query.OrderBy...)
	}

	if len(query.Search) > 0 {
		if v, args, err := query.Search.WhereString(nil); err != nil {
			return nil, errors.Wrap(err, "invalid search")
		} else {
			searchString = v
			searchArgs = args
			// utils.GetLogger().Warn("debug", zap.String("where", searchString), zap.Any("args", args))
		}
	}

	if query.Offset > 0 {
		offsetClause = append(offsetClause, fmt.Sprintf("OFFSET %d", query.Offset))
	}
	if query.Limit > 0 {
		limitClause = append(limitClause, fmt.Sprintf("LIMIT %d", query.Limit))
	}
	// Construct the WHERE clause of the SQL query based on the query parameters.
	where := fmt.Sprintf("WHERE time >= to_timestamp(%d) AND time < to_timestamp(%d)", from, to+1)
	if len(whereClause) > 0 {
		where += " AND " + strings.Join(whereClause, " AND ")
	}
	if searchString != "" {
		where += " AND (" + searchString + ")"
	}

	// Construct the GROUP BY clause of the SQL query based on the GroupBy fields in the query.
	groupBy := "GROUP BY t"
	if len(groupByClause) > 0 {
		groupBy = groupBy + ", " + strings.Join(groupByClause, ", ")
	}

	// Construct the ORDER BY clause of the SQL query based on the OrderBy fields in the query.
	orderBy := ""
	if len(orderByClause) > 0 {
		orderBy = "ORDER BY " + strings.Join(orderByClause, ", ")
	}

	// Construct the SQL query.
	queryStr := fmt.Sprintf("SELECT %s as t, %s FROM %s %s %s %s %s %s",
		bucketFunc,
		strings.Join(selectClause, ", "),
		query.Source,
		where,
		groupBy,
		orderBy,
		strings.Join(offsetClause, " "),
		strings.Join(limitClause, " "),
	)

	// Execute the SQL query and scan the result set into a slice of TSSeries structs.
	rows, err := db.Debug().Raw(queryStr, searchArgs...).Rows()
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	defer rows.Close()

	result := make([]TSSeries, 0)
	seriesMap := make(map[string]*TSSeries)
	indexMap := make(map[string]int)
	for rows.Next() {
		var ts time.Time
		values := make([]any, len(query.Select)+1)
		values[0] = &ts
		for i, sel := range query.Select {
			indexMap[sel.Name] = i
			if sel.Func == "text" {
				values[i+1] = new(string)
			} else {
				values[i+1] = new(sql.NullFloat64)
			}
		}
		if err := rows.Scan(values...); err != nil {
			return nil, errors.Wrap(err, "scan rows")
		}
		seriesKey := query.Id
		groupByMap := make(map[string]string)
		if len(query.GroupBy) > 0 {
			for _, gb := range query.GroupBy {
				if idx, ok := indexMap[gb]; ok && idx+1 < len(values) {
					groupByMap[gb] = cast.ToString(values[idx+1])
					seriesKey = fmt.Sprintf("%s|%v", seriesKey, groupByMap[gb])
				}
			}
		}
		point := TSPoint{}
		point["time"] = ts.Unix()
		for i, sel := range query.Select {
			value := values[i+1]
			if value == nil {
				point[sel.Name] = nil
				continue
			}
			switch v := value.(type) {
			case string:
				point[sel.Name] = cast.ToString(v)
			case *string:
				point[sel.Name] = cast.ToString(v)
			case time.Time:
				point[sel.Name] = v.Unix()
			case *time.Time:
				point[sel.Name] = v.Unix()
			case sql.NullFloat64:
				if v.Valid {
					point[sel.Name] = v.Float64
				} else {
					point[sel.Name] = "invalid"
				}
			case *sql.NullFloat64:
				if v.Valid {
					point[sel.Name] = v.Float64
				} else {
					point[sel.Name] = "invalid"
				}
			default:
				point[sel.Name] = cast.ToString(v)
			}
		}
		series, ok := seriesMap[seriesKey]
		if !ok {
			series = &TSSeries{
				GroupBy: groupByMap,
				Points:  make([]TSPoint, 0),
			}
			seriesMap[seriesKey] = series
		}
		series.Points = append(series.Points, point)
	}
	for _, series := range seriesMap {
		result = append(result, *series)
	}
	return result, nil
}
