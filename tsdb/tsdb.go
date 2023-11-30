package tsdb

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

func CreateHyperTable(db *gorm.DB, tableName string, dataRetentionPeriod time.Duration) error {
	q := fmt.Sprintf(
		`SELECT public.create_hypertable('%v', 'time', chunk_time_interval => 86400000000, if_not_exists => TRUE);`,
		tableName,
	)
	if result := db.Exec(q); result.Error != nil {
		return result.Error
	}
	SetDataRetentionPolicyForHyperTalbe(db, tableName, dataRetentionPeriod)
	return nil
}

func SetDataRetentionPolicyForHyperTalbe(db *gorm.DB, tableName string, duration time.Duration) error {
	db.Exec(fmt.Sprintf("SELECT public.remove_retention_policy('%v', if_exists => TRUE);", tableName))
	if duration > 0 {
		q := fmt.Sprintf("SELECT public.add_retention_policy('%v', INTERVAL '%v');", tableName, int64(duration.Seconds()))
		if result := db.Exec(q); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func parseTimeBucket(v string) time.Duration {
	pattern := `(\d+)([hmd])`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(v)
	if len(matches) != 3 {
		return 0
	}
	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0
	}
	unit := matches[2]
	switch unit {
	case "h":
		return time.Duration(value) * time.Hour
	case "m":
		return time.Duration(value) * time.Minute
	case "d":
		return time.Duration(value) * 24 * time.Hour
	}
	return 0
}

type continuousAggregatePolicyOptions struct {
	TimeBucket       string
	TimeDuration     time.Duration
	StartOffset      string
	EndOffset        string
	ScheduleInterval string
}

func newContinuousAggregatePolicyOptions(v string) continuousAggregatePolicyOptions {
	opts := continuousAggregatePolicyOptions{}
	opts.TimeDuration = parseTimeBucket(v)
	opts.TimeBucket = v
	if opts.TimeDuration < time.Minute {
		opts.TimeDuration = time.Minute
		opts.TimeBucket = "1m"
	}
	if opts.TimeDuration <= 10*time.Minute {
		opts.StartOffset = "30m"
		opts.EndOffset = "1m"
		opts.ScheduleInterval = "1m"
	} else if opts.TimeDuration <= time.Hour {
		opts.StartOffset = "3h"
		opts.EndOffset = "5m"
		opts.ScheduleInterval = "5m"
	} else {
		opts.StartOffset = "3d"
		opts.EndOffset = "30m"
		opts.ScheduleInterval = "30m"
	}
	return opts
}

func CreateHyperTableCountView(db *gorm.DB, tableName string, viewName string, timeBucket string, indexColumns []string) error {
	opts := newContinuousAggregatePolicyOptions(timeBucket)
	indexColumnsString := strings.Join(indexColumns, ",")
	q := fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %v(time, %v, "count")
 WITH (timescaledb.continuous) AS
 SELECT 
   public.time_bucket('%v', time),
	 %v,
	 count(time)
 FROM %v
 GROUP BY public.time_bucket('%v', time), %v`,
		viewName, indexColumnsString,
		timeBucket, indexColumnsString,
		tableName, opts.TimeBucket, indexColumnsString,
	)
	if result := db.Exec(q); result.Error != nil {
		return result.Error
	}

	q2 := fmt.Sprintf(`SELECT public.remove_continuous_aggregate_policy('%v',if_exists => TRUE);`, viewName)
	db.Exec(q2)

	q2 = fmt.Sprintf(`SELECT public.add_continuous_aggregate_policy('%v',
 if_not_exists => TRUE,
 start_offset => INTERVAL '%v',
 end_offset => INTERVAL '%v',
 schedule_interval => INTERVAL '%v');`,
		viewName, opts.StartOffset, opts.EndOffset, opts.ScheduleInterval,
	)
	if result := db.Exec(q2); result.Error != nil {
		return result.Error
	}
	return nil
}

func DropHyperTableView(db *gorm.DB, viewName string) error {
	q := fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %v`, viewName)
	if result := db.Exec(q); result.Error != nil {
		return result.Error
	}
	return nil
}

func CreateHyperTableAvgValuesView(db *gorm.DB, tableName string, viewName string, timeBucket string,
	nameColumn string, valueColumn string, indexColumns []string, names []string, where string) error {
	_names := []string{}
	for _, v := range names {
		_names = append(_names, strings.Replace(v, ".", "_", -1))
	}

	opts := newContinuousAggregatePolicyOptions(timeBucket)
	indexColumnsString := strings.Join(indexColumns, ",")
	namesString := strings.Join(_names, ",")
	values := []string{}
	for _, name := range _names {
		value := fmt.Sprintf("avg(CASE WHEN %v = '%v' THEN %v END) as %v", nameColumn, name, valueColumn, strings.Replace(name, ".", "_", -1))
		values = append(values, value)
	}
	valuesString := strings.Join(values, ",")

	whereArg := ""
	if where != "" {
		whereArg = "WHERE " + where
	}
	q := fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %v(time, %v, %v)
	WITH (timescaledb.continuous) AS
	SELECT  public.time_bucket('%v', time), %v, %v
	FROM %v
	%v 
	GROUP BY public.time_bucket('%v', time), %v`,
		viewName, indexColumnsString, namesString,
		timeBucket, indexColumnsString, valuesString,
		tableName,
		whereArg,
		opts.TimeBucket, indexColumnsString,
	)

	if result := db.Exec(q); result.Error != nil {
		return result.Error
	}

	q2 := fmt.Sprintf(`SELECT public.remove_continuous_aggregate_policy('%v',if_exists => TRUE);`, viewName)
	db.Exec(q2)

	q2 = fmt.Sprintf(`SELECT public.add_continuous_aggregate_policy('%v',
	 if_not_exists => TRUE,
	 start_offset => INTERVAL '%v',
	 end_offset => INTERVAL '%v',
	 schedule_interval => INTERVAL '%v');`,
		viewName, opts.StartOffset, opts.EndOffset, opts.ScheduleInterval,
	)
	if result := db.Exec(q2); result.Error != nil {
		return result.Error
	}
	return nil
}
