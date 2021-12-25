package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	q "github.com/xdqc/gremlinifier/query"
)

var (
	db *sql.DB
)

func main() {
	q.Init(true)
	connectSQL()
	// addEdgePathologyReportDiagnose()
	// err := q.QueryCosmos("g.V().haslabel('')")
	// count, err :=
	// if err != nil {
	// 	log.Fatal("Error reading entries: ", err.Error())
	// }
	// log.Printf("Read %d row(s) with error.\n", count)
}

func connectSQL() {
	var err error
	connStr := os.Getenv("SQLDB_CONN")
	db, err = sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal("PingContext: ", err.Error())
	}
	q.Logger.Debug().Msg("Connected!\n")
}

func addPatients() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
	SELECT 
  FROM`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		patient := make([]interface{}, 7)
		err := rows.Scan(
			&patient[0],
			&patient[1],
			&patient[2],
			&patient[3],
			&patient[4],
			&patient[5],
			&patient[6],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", patient)
		gremlin := fmt.Sprintf(`g.addV('patient')
			.property('id', '%s')
			.property('name', '%s')
			.property('sex', '%s')
			.property('pk', '%v')
			`,
			patient[0], patient[1], patient[3], patient[0])
		if patient[4] != nil {
			gremlin += fmt.Sprintf(".property('ageFirstDiagnose', %d)", patient[4])
		}
		if patient[5] != nil {
			gremlin += fmt.Sprintf(".property('ageDeath', %d)", patient[5])
		}
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func readCauseOfDeath() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
	SELECT
  FROM
	WHERE`
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		entry := make([]interface{}, 1)
		err := rows.Scan(
			&entry[0],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", entry)
		cause_of_death := escapeStr(entry[0])
		gremlin := fmt.Sprintf(`g.addV('cause_of_death')
			.property('name', '%s')
			.property('pk', 'cause_of_death')
			`, cause_of_death)

		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func escapeStr(entry interface{}) string {
	if entry == nil {
		return ""
	}
	escapedStr := fmt.Sprintf("%s", entry)
	escapedStr = strings.TrimSpace(escapedStr)
	escapedStr = strings.ReplaceAll(escapedStr, "'", "\\'")
	return escapedStr
}

func sqlDate2Str(entry interface{}) string {
	dateStr := fmt.Sprintf("%v", entry)
	return dateStr[:10] // YYYY-MM-DD
}

func getTreatment(treatment_code string) string {
	treatment := map[string]string{
		"9": "__explorativesurgery",
		"1": "__surgery",
		"2": "_radiotherapy",
		"3": "_radioisotopes",
		"7": "_rehabilitation",
		"4": "chemotherapy",
		"5": "hormontherapy",
		"6": "immunotherapy",
		"0": "notreatment",
		"8": "othertherapy",
		"X": "unknownsystemictreatment",
	}
	return treatment[treatment_code]
}

func readICD10() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
	SELECT
	FROM
  WHERE
  ORDER BY
	`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	cols := make([]interface{}, 7)
	for rows.Next() {
		err := rows.Scan(
			&cols[0],
			&cols[1],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g.addV('icd10')
			.property('name', '%s')
			.property('pk', '%s')
			`, cols[0], fmt.Sprint(cols[0])[0:1])
		if cols[1] != nil {
			gremlin += fmt.Sprintf(".property('oms', '%s')", escapeStr(cols[1]))
		}
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func addPrimaryTumor() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
SELECT
  FROM 
  OUTER APPLY (
		SELECT 
		WHERE 
  ) a
  OUTER APPLY (
		SELECT 
		FROM 
		WHERE
  ) t
	WHERE
	`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	cols := make([]interface{}, 16)
	for rows.Next() {
		err := rows.Scan(
			&cols[0],
			&cols[1],
			&cols[2],
			&cols[3],
			&cols[4],
			&cols[5],
			&cols[6],
			&cols[7],
			&cols[8],
			&cols[9],
			&cols[10],
			&cols[11],
			&cols[12],
			&cols[13],
			&cols[14],
			&cols[15],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g.addV('primary_tumor')
			.property('name', '%s')
			.property('pk', '%s')
			`, fmt.Sprintf("%s_%d", cols[1], cols[2]), cols[0])
		if cols[3] != nil {
			gremlin += fmt.Sprintf(".property('diagnosis', '%s')", escapeStr(cols[3]))
		}
		if cols[5] != nil {
			gremlin += fmt.Sprintf(".property('intake_date', '%s')", sqlDate2Str(cols[5]))
		}
		if cols[13] != nil {
			gremlin += fmt.Sprintf(".property('diagnose_date', '%s')", sqlDate2Str(cols[13]))
		}
		if cols[6] != nil {
			gremlin += fmt.Sprintf(".property('clinical_stage', '%s')", escapeStr(cols[6]))
		}
		if cols[7] != nil {
			gremlin += fmt.Sprintf(".property('cT', '%s')", escapeStr(cols[7]))
		}
		if cols[8] != nil {
			gremlin += fmt.Sprintf(".property('cN', '%s')", escapeStr(cols[8]))
		}
		if cols[9] != nil {
			gremlin += fmt.Sprintf(".property('cM', '%s')", escapeStr(cols[9]))
		}
		if cols[10] != nil {
			gremlin += fmt.Sprintf(".property('pT', '%s')", escapeStr(cols[10]))
		}
		if cols[11] != nil {
			gremlin += fmt.Sprintf(".property('pN', '%s')", escapeStr(cols[11]))
		}
		if cols[12] != nil {
			gremlin += fmt.Sprintf(".property('pM', '%s')", escapeStr(cols[12]))
		}
		// query.Logger.Info().Msg(gremlin)
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func addPathology() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `SELECT 
  FROM 
  OUTER APPLY (
	SELECT 
	WHERE
  ) a
  WHERE
`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		cols := make([]interface{}, 7)
		err := rows.Scan(
			&cols[0],
			&cols[1],
			&cols[2],
			&cols[3],
			&cols[4],
			&cols[5],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g.addV('pathology_analysis')
		.property('name', '%s')
		.property('pk', '%s')
			`, escapeStr(cols[2]), cols[0])
		if cols[4] != nil {
			gremlin += fmt.Sprintf(".property('sample_date', '%s')", sqlDate2Str(cols[4]))
		}
		if cols[5] != nil {
			gremlin += fmt.Sprintf(".property('report_date', '%s')", sqlDate2Str(cols[5]))
		}
		if cols[3] != nil {
			gremlin += fmt.Sprintf(".property('diagnose_code', '%s')", escapeStr(cols[3]))
		}

		// gremlin += fmt.Sprintf(`.as('pa')
		// 	.V().haslabel('patient').has('name', '%s')
		// 	.coalesce(__.outE('has_pathology') ,
		// 						__.addE('has_pathology').to('pa'))
		// `, cols[1])

		// query.Logger.Info().Msg(gremlin)
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func addTherapyTypes() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `SELECT
	FROM 
	WHERE 
  ORDER BY
`
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		cols := make([]interface{}, 4)
		err := rows.Scan(
			&cols[0],
			&cols[1],
			&cols[2],
			&cols[3],
		)
		if err != nil {
			return -1, err
		}
		// q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g.addV('therapy_type').property('pk', '%s').property('name', '%s')
		.property('treatment_code', '%s').property('treatment_oms', '%s')
		.property('therapy_code', '%s').property('therapy_oms', '%s')
			`, escapeStr(cols[0])+"_"+escapeStr(cols[1]), escapeStr(cols[0])+"_"+escapeStr(cols[2]),
			cols[0], escapeStr(cols[1]),
			cols[2], escapeStr(cols[3]),
		)
		// q.Logger.Debug().Msg(gremlin)
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

func addTreatment() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `SELECT
  FROM 
  CROSS APPLY (
  SELECT
	FROM 
	WHERE 
  ) t
`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		cols := make([]interface{}, 18)
		err := rows.Scan(
			&cols[0],
			&cols[1],
			&cols[2],
			&cols[3],
			&cols[4],
			&cols[5],
			&cols[6],
			&cols[7],
			&cols[8],
			&cols[9],
			&cols[10],
			&cols[11],
			&cols[12],
			&cols[13],
			&cols[14],
			&cols[15],
			&cols[16],
			&cols[17],
		)
		if err != nil {
			return -1, err
		}
		// q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g.addV('treatment').property('pk', '%s').property('name', '%s')
			.property('treated_at', '%s').property('treatment_code', '%s').property('treatment_oms', '%s')
			.property('therapy_code', '%s').property('therapy_oms', '%s')
			`, cols[0], fmt.Sprintf("%s_%d_%s", escapeStr(cols[1]), cols[3], getTreatment(escapeStr(cols[5]))),
			cols[4], escapeStr(cols[5]), escapeStr(cols[6]),
			escapeStr(cols[7]), escapeStr(cols[8]))
		if cols[9] != nil {
			gremlin += fmt.Sprintf(".property('start_date', '%s')", sqlDate2Str(cols[9]))
		}
		if cols[10] != nil {
			gremlin += fmt.Sprintf(".property('stop_date', '%s')", sqlDate2Str(cols[10]))
		}
		if escapeStr(cols[11]) != "" {
			gremlin += fmt.Sprintf(".property('radiotherapy_location_code', '%s')", escapeStr(cols[11]))
		}
		if escapeStr(cols[12]) != "" {
			gremlin += fmt.Sprintf(".property('radiotherapy_location_oms', '%s')", escapeStr(cols[12]))
		}
		if escapeStr(cols[13]) != "" {
			number_course, err := strconv.Atoi(escapeStr(cols[13]))
			if err != nil {
				log.Panic(err)
			} else {
				gremlin += fmt.Sprintf(".property('number_courses', %d)", number_course)
			}
		}
		if escapeStr(cols[14]) != "" {
			gremlin += fmt.Sprintf(".property('medicin_1', '%s')", escapeStr(cols[14]))
		}
		if escapeStr(cols[15]) != "" {
			gremlin += fmt.Sprintf(".property('medicin_2', '%s')", escapeStr(cols[15]))
		}
		if escapeStr(cols[16]) != "" {
			gremlin += fmt.Sprintf(".property('medicin_3', '%s')", escapeStr(cols[16]))
		}
		if escapeStr(cols[17]) != "" {
			gremlin += fmt.Sprintf(".property('medicin_4', '%s')", escapeStr(cols[17]))
		}
		q.Logger.Info().Msgf("%v", gremlin)
		// err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

//FIXME: wrong direction. connotation -> denotation, should be cause_of_death--die_from-->patient
//Patient--die_of-->CauseOfDeath
func addEdgePatientCauseOfDeath() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
	SELECT 
  FROM 
	WHERE `

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		patient := make([]interface{}, 7)
		err := rows.Scan(
			&patient[0],
			&patient[1],
			&patient[2],
			&patient[3],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", patient)
		gremlin := fmt.Sprintf(`g.V().haslabel('cause_of_death').has('name', '%s')
			.addE('die_of').from(g.V().haslabel('patient').has('name', '%s'))
			`, escapeStr(patient[3]), patient[0])
		if patient[2] != nil {
			gremlin += fmt.Sprintf(".property('ageDeath', %d)", patient[2])
		}
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

//Patient--[eLabel]-->[Vlabel]
func addEdgePatient2(patientToVLabel string, eLabel string) (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
	SELECT 
  FROM `

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		patient := make([]interface{}, 2)
		err := rows.Scan(
			&patient[0],
			&patient[1],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", patient)
		gremlin := fmt.Sprintf(`g
.V().haslabel('patient').has('name', '%s').as('p')
.V().haslabel('%s').has('pk', '%s')
.coalesce(__.inE('%s'),
					__.addE('%s').from('p'))
			`, patient[1], patientToVLabel, patient[0], eLabel, eLabel)

		// query.Logger.Info().Msg(gremlin)
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

//PathologyAnalysis--pathology_diagnose_tumor-->PrimaryTumor
func addEdgePathology2Tumor() (int, error) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	tsql := `
SELECT 
  FROM 
  OUTER APPLY (
		SELECT 
		WHERE 
  ) a
  OUTER APPLY (
		SELECT
		FROM 
		WHERE 
  ) t
	WHERE 
		AND 
		AND 
	`

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var count int
	cols := make([]interface{}, 6)
	for rows.Next() {
		err := rows.Scan(
			&cols[0],
			&cols[1],
			&cols[2],
			&cols[3],
			&cols[4],
			&cols[5],
		)
		if err != nil {
			return -1, err
		}
		q.Logger.Info().Msgf("%v", cols)
		gremlin := fmt.Sprintf(`g
		.V().haslabel('pathology_analysis').has('name', '%s').as('pa')
		.V().haslabel('primary_tumor').has('name', '%s').as('tu')
		.coalesce(
			__.inE('pathology_diagnose_tumor'),
			__.addE('pathology_diagnose_tumor').from('pa')
		)`, escapeStr(cols[4]), fmt.Sprintf("%s_%d", cols[1], cols[2]))

		// query.Logger.Info().Msg(gremlin)
		err = q.QueryCosmos(gremlin)
		if err != nil {
			count++
		}
	}
	return count, err
}

//PrimaryTumor--diagnose_of-->ICD10
func addEdgeTumorDiagnose() {
	tumors := q.QueryCosmosValues("g.V().haslabel('primary_tumor').project('properties').by(__.valueMap())")
	for _, t := range tumors {
		tumor := t.Value.(map[string]interface{})["properties"]
		tumor_name := tumor.(map[string]interface{})["name"].([]interface{})[0]
		diagnosis := tumor.(map[string]interface{})["diagnosis"].([]interface{})[0]
		diagnose_dates := tumor.(map[string]interface{})["diagnose_date"]
		intake_dates := tumor.(map[string]interface{})["intake_date"]
		gremlin := fmt.Sprintf(`g
		.V().haslabel('icd10').has('name', '%s').as('diag')
		.V().haslabel('primary_tumor').has('name', '%s').as('tu')
		.coalesce(
			__.outE('diagnose_of'),
			__.addE('diagnose_of').to('diag')
		)`, diagnosis, tumor_name)
		if diagnose_dates != nil {
			gremlin += fmt.Sprintf(".property('diagnose_date', '%s')", diagnose_dates.([]interface{})[0])
		}
		if intake_dates != nil {
			gremlin += fmt.Sprintf(".property('intake_date', '%s')", intake_dates.([]interface{})[0])
		}
		// q.Logger.Debug().Msg(gremlin)
		q.QueryCosmos(gremlin)
	}
}

//Patient--has_pathology-->PathologyAnalysis
func addEdgePatientHasPathology() {
	pathologies := q.QueryCosmosValues("g.V().haslabel('pathology_analysis').project('id','properties').by(__.id()).by(__.valueMap())")
	for _, pa := range pathologies {
		paid := pa.Value.(map[string]interface{})["id"]
		pp := pa.Value.(map[string]interface{})["properties"]
		pk := pp.(map[string]interface{})["pk"].([]interface{})[0]
		// paname := pp.(map[string]interface{})["name"].([]interface{})[0]
		sample_date := pp.(map[string]interface{})["sample_date"].([]interface{})[0]
		report_date := pp.(map[string]interface{})["report_date"]

		gremlin := fmt.Sprintf(`g.V().has('id', '%s').as('pa')
		.coalesce(
			 __.inE('has_pathology'),
			__.addE('has_pathology').from(__.V().has('id','%s'))
		).property('sample_date', '%s')`, paid, pk, sample_date)
		if report_date != nil {
			gremlin += fmt.Sprintf(".property('report_date', '%s')", report_date.([]interface{})[0])
		}
		// q.Logger.Debug().Msgf(gremlin)
		q.QueryCosmos(gremlin)
	}
}

//PathologyAnalysis--pathology_report_diagnose-->ICD10
func addEdgePathologyReportDiagnose() {
	pathologies := q.QueryCosmosValues("g.V().haslabel('pathology_analysis').project('id','properties').by(__.id()).by(__.valueMap())")
	for _, pa := range pathologies {
		paid := pa.Value.(map[string]interface{})["id"]
		pp := pa.Value.(map[string]interface{})["properties"]
		pk := pp.(map[string]interface{})["pk"].([]interface{})[0]
		// paname := pp.(map[string]interface{})["name"].([]interface{})[0]
		sample_date := pp.(map[string]interface{})["sample_date"].([]interface{})[0]
		report_date := pp.(map[string]interface{})["report_date"]
		gremlin := fmt.Sprintf(`g.V().has('id', '%s').as('pa').outE().haslabel('pathology_diagnose_tumor').select('pa')
		.coalesce(
			__.outE('pathology_report_diagnose'),
			__.addE('pathology_report_diagnose').to(__.V().has('id','%s'))
		).property('sample_date', '%s')`, paid, pk, sample_date)
		if report_date != nil {
			gremlin += fmt.Sprintf(".property('report_date', '%s')", report_date.([]interface{})[0])
		}
		// q.Logger.Debug().Msgf(gremlin)
		q.QueryCosmos(gremlin)
	}
}

//PathologyAnalysis(later)--follow_pathology-->PathologyAnalysis(earlier)
func addEdgePathologyFollow(pType string) {
	patients := q.QueryCosmosValues("g.V().haslabel('patient').properties('pk').value()")
	for _, p := range patients {
		pk := fmt.Sprintf("%s", p.Value)
		gremlin := fmt.Sprintf("g.V().has('pk', '%s').haslabel('pathology_analysis').has('name', TextP.startingWith('%s')).order().by('sample_date').properties('sample_date').value()",
			pk, pType)
		pathos_dates := q.QueryCosmosValues(gremlin)
		prev_date := ""
		for i, p := range pathos_dates {
			sample_date := fmt.Sprintf("%s", p.Value)
			if prev_date != "" && prev_date != sample_date {
				gremlin = fmt.Sprintf(`g
				.V().has('pk', '%s').has('name', TextP.startingWith('%s')).has('sample_date','%s').as('op')
				.V().has('pk', '%s').has('name', TextP.startingWith('%s')).has('sample_date','%s').as('np')
				.coalesce(
					__.outE('follow_pathology'),
					__.addE('follow_pathology').to('op')
				)`, pk, pType, prev_date, pk, pType, sample_date)
				q.Logger.Info().Msgf("%s %d %v", pk, i, sample_date)
				q.QueryCosmos(gremlin)
			}
			prev_date = sample_date
		}
	}
}

//Treatment(later)--follow_treatment-->Treatment(earlier)
func addEdgeTreatmentFollow() {
	patients := q.QueryCosmosValues("g.V().haslabel('patient').properties('pk').value()")
	for _, p := range patients {
		pk := fmt.Sprintf("%s", p.Value)
		gremlin := fmt.Sprintf(`g.V().has('pk', '%s').haslabel('treatment').has('start_date')
		.order().by(coalesce(values('start_date'), constant(''))).by('name').properties('id').value()`, pk)
		th_ids := q.QueryCosmosValues(gremlin)
		prev_id := ""
		for _, td := range th_ids {
			tid := fmt.Sprintf("%s", td.Value)
			if prev_id != "" && prev_id != tid {
				gremlin = fmt.Sprintf(`g
					.V().has('pk', '%s').has('id','%s').as('ot')
					.V().has('pk', '%s').has('id','%s').as('nt')
					.coalesce(
						__.outE('follow_treatment'),
						__.addE('follow_treatment').to('ot')
					)`, pk, prev_id, pk, tid)
				// q.Logger.Debug().Msg(gremlin)
				q.QueryCosmos(gremlin)
			}
			prev_id = tid
		}
	}
}

func updateTreatmentName() {
	treatments := q.QueryCosmosValues("g.V().haslabel('treatment').project('id','properties').by(__.id()).by(__.valueMap())")
	for _, th := range treatments {
		thid := th.Value.(map[string]interface{})["id"]
		tp := th.Value.(map[string]interface{})["properties"]
		thcode := tp.(map[string]interface{})["treatment_code"].([]interface{})[0]
		thname := tp.(map[string]interface{})["name"].([]interface{})[0]
		tuname := strings.Join(strings.Split(thname.(string), "_")[:3], "_") // XYZ_ABC123_1
		trname := tuname + "_" + getTreatment(thcode.(string))
		gremlin := fmt.Sprintf(`g.V().has('id', '%s').property('name', '%s')`, thid, trname)
		// q.Logger.Debug().Msg(gremlin)
		q.QueryCosmos(gremlin)
	}
}

//PrimaryTumor--treated_by-->Treatment
func addEdgeTreatmentTumor() {
	treatments := q.QueryCosmosValues("g.V().haslabel('treatment').project('id','properties').by(__.id()).by(__.valueMap())")
	for _, th := range treatments {
		thid := th.Value.(map[string]interface{})["id"]
		tp := th.Value.(map[string]interface{})["properties"]
		thname := tp.(map[string]interface{})["name"].([]interface{})[0]
		tuname := strings.Join(strings.Split(thname.(string), "_")[:3], "_") // XYZ_ABC123_1
		start_date := tp.(map[string]interface{})["start_date"]
		stop_date := tp.(map[string]interface{})["stop_date"]
		gremlin := fmt.Sprintf(`g
			.V().haslabel('primary_tumor').has('name', '%s').as('tu')
			.V().haslabel('treatment').has('id', '%s').as('th')
			.coalesce(
				__.inE('treated_by'),
				__.addE('treated_by').from('tu')
			)`, tuname, thid)
		if start_date != nil {
			gremlin += fmt.Sprintf(".property('start_date', '%s')", start_date.([]interface{})[0])
		}
		if stop_date != nil {
			gremlin += fmt.Sprintf(".property('stop_date', '%s')", stop_date.([]interface{})[0])
		}
		// q.Logger.Debug().Msg(gremlin)
		q.QueryCosmos(gremlin)
	}
}
