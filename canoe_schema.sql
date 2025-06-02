CREATE TABLE IF NOT EXISTS "time_season" (
	"t_season"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("t_season")
);
CREATE TABLE IF NOT EXISTS "time_periods" (
	"t_periods"	integer,
	"flag"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("t_periods"),
	FOREIGN KEY("flag") REFERENCES "time_period_labels"("t_period_labels")
);
CREATE TABLE IF NOT EXISTS "time_period_labels" (
	"t_period_labels"	text,
	"t_period_labels_desc"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("t_period_labels")
);
CREATE TABLE IF NOT EXISTS "time_of_day" (
	"t_day"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("t_day")
);
CREATE TABLE IF NOT EXISTS "technology_labels" (
	"tech_labels"	text,
	"tech_labels_desc"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech_labels")
);
CREATE TABLE IF NOT EXISTS "technologies" (
	"tech"	text,
	"flag"	text,
	"sector"	text,
	"tech_desc"	text,
	"tech_category"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech"),
	FOREIGN KEY("flag") REFERENCES "technology_labels"("tech_labels"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector")
);
CREATE TABLE IF NOT EXISTS "tech_ramping" (
	"tech"	text, notes text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech")
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "tech_reserve" (
	"tech"	text,
	"notes"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech")
);
CREATE TABLE IF NOT EXISTS "tech_exchange" (
	"tech"	text,
	"notes"	TEXT,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "tech_curtailment" (
	"tech"	text,
	"notes"	TEXT,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "tech_flex" (
	"tech"	text,
	"notes"	TEXT,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "tech_annual" (
	"tech"	text,
	"notes"	TEXT,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "sector_labels" (
	"sector"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("sector")
);
CREATE TABLE IF NOT EXISTS "regions" (
	"regions"	TEXT,
	"region_note"	TEXT,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("regions")
);
CREATE TABLE IF NOT EXISTS "groups" (
	"group_name"	text,
	"notes"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("group_name")
);
CREATE TABLE IF NOT EXISTS "commodity_labels" (
	"comm_labels"	text,
	"comm_labels_desc"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("comm_labels")
);
CREATE TABLE IF NOT EXISTS "commodities" (
	"comm_name"	text,
	"flag"	text,
	"comm_desc"	text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),
	PRIMARY KEY("comm_name"),
	FOREIGN KEY("flag") REFERENCES "commodity_labels"("comm_labels")
);
CREATE TABLE IF NOT EXISTS "TechOutputSplit" (
	"regions"	TEXT,
	"periods"	integer,
	"tech"	TEXT,
	"output_comm"	text,
	"to_split"	real,
	"to_split_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech","output_comm"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "TechInputSplit" (
	"regions"	TEXT,
	"periods"	integer,
	"input_comm"	text,
	"tech"	text,
	"ti_split"	real,
	"ti_split_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","input_comm","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "TechInputSplitAverage" (
	"regions"	TEXT,
	"periods"	integer,
	"input_comm"	text,
	"tech"	text,
	"ti_split"	real,
	"ti_split_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","input_comm","tech"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "StorageDuration" (
	"regions"	text,
	"tech"	text,
	"duration"	real,
	"duration_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech")
);
CREATE TABLE IF NOT EXISTS "SegFrac" (
	"season_name"	text,
	"time_of_day_name"	text,
	"segfrac"	real CHECK("segfrac" >= 0 AND "segfrac" <= 1),
	"segfrac_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("season_name","time_of_day_name"),
	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),
	FOREIGN KEY("time_of_day_name") REFERENCES "time_of_day"("t_day")
);
CREATE TABLE IF NOT EXISTS "PlanningReserveMargin" (
	`regions`	text,
	`reserve_margin`	REAL,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY(regions),
	FOREIGN KEY(`regions`) REFERENCES regions
);
CREATE TABLE IF NOT EXISTS "RampDown" (
	`regions`	text,
	`tech`	text,
	`ramp_down` real,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions", "tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "RampUp" (
	`regions`	text,
	`tech`	text,
	`ramp_up` real,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions", "tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "Output_V_Capacity" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"tech"	text,
	"vintage"	integer,
	"capacity"	real,
	PRIMARY KEY("regions","scenario","tech","vintage"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "Output_VFlow_Out" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"t_periods"	integer,
	"t_season"	text,
	"t_day"	text,
	"input_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"output_comm"	text,
	"vflow_out"	real,
	PRIMARY KEY("regions","scenario","t_periods","t_season","t_day","input_comm","tech","vintage","output_comm"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("t_periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("t_season") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("t_day") REFERENCES "time_of_day"("t_day"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "Output_VFlow_In" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"t_periods"	integer,
	"t_season"	text,
	"t_day"	text,
	"input_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"output_comm"	text,
	"vflow_in"	real,
	PRIMARY KEY("regions","scenario","t_periods","t_season","t_day","input_comm","tech","vintage","output_comm"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("t_periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("t_season") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("t_day") REFERENCES "time_of_day"("t_day"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "Output_Objective" (
	"scenario"	text,
	"objective_name"	text,
	"total_system_cost"	real
);
CREATE TABLE IF NOT EXISTS "Output_Emissions" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"t_periods"	integer,
	"emissions_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"emissions"	real,
	PRIMARY KEY("regions","scenario","t_periods","emissions_comm","tech","vintage"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("emissions_comm") REFERENCES "EmissionActivity"("emis_comm"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("t_periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "Output_Curtailment" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"t_periods"	integer,
	"t_season"	text,
	"t_day"	text,
	"input_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"output_comm"	text,
	"curtailment"	real,
	PRIMARY KEY("regions","scenario","t_periods","t_season","t_day","input_comm","tech","vintage","output_comm"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("t_periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("t_season") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("t_day") REFERENCES "time_of_day"("t_day")
);
CREATE TABLE IF NOT EXISTS "Output_Costs" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"output_name"	text,
	"tech"	text,
	"vintage"	integer,
	"output_cost"	real,
	PRIMARY KEY("regions","scenario","output_name","tech","vintage"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "Output_Duals" (
	"constraint_name"	text,
	"scenario"	text,
	"dual"	real,
	PRIMARY KEY("constraint_name","scenario")
);
CREATE TABLE IF NOT EXISTS "Output_CapacityByPeriodAndTech" (
	"regions"	text,
	"scenario"	text,
	"sector"	text,
	"t_periods"	integer,
	"tech"	text,
	"capacity"	real,
	PRIMARY KEY("regions","scenario","t_periods","tech"),
	FOREIGN KEY("sector") REFERENCES "sector_labels"("sector"),
	FOREIGN KEY("t_periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "MyopicBaseyear" (
	"year"	real
	"notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
);
CREATE TABLE IF NOT EXISTS "MinGenGroupWeight" (
	"regions"	text,
	"tech"	text,
	"group_name"	text,
	"act_fraction"	REAL,
	"tech_desc"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("tech","group_name","regions")
);
CREATE TABLE IF NOT EXISTS "MinGenGroupTarget" (
	"regions"	text,
	"periods"	integer,
	"group_name"	text,
	"min_act_g"	real,
	"notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("periods","group_name","regions")
);
CREATE TABLE IF NOT EXISTS "MinCapacity" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"mincap"	real,
	"mincap_units"	text,
	"mincap_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "MinActivity" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"minact"	real,
	"minact_units"	text,
	"minact_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "MaxCapacity" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"maxcap"	real,
	"maxcap_units"	text,
	"maxcap_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "MaxActivity" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"maxact"	real,
	"maxact_units"	text,
	"maxact_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "MinAnnualCapacityFactor" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"output_comm" text,
	"min_acf"	real CHECK("min_acf" >= 0 AND "min_acf" <= 1),
	"min_acf_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")

	PRIMARY KEY("regions","periods","tech","output_comm"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "MaxAnnualCapacityFactor" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"output_comm" text,
	"max_acf"	real CHECK("max_acf" >= 0 AND "max_acf" <= 1),
	"max_acf_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")

	PRIMARY KEY("regions","periods","tech","output_comm"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "LifetimeTech" (
	"regions"	text,
	"tech"	text,
	"life"	real,
	"life_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "LifetimeProcess" (
	"regions"	text,
	"tech"	text,
	"vintage"	integer,
	"life_process"	real,
	"life_process_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech","vintage"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "LifetimeLoanTech" (
	"regions"	text,
	"tech"	text,
	"loan"	real,
	"loan_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "GrowthRateSeed" (
	"regions"	text,
	"tech"	text,
	"growthrate_seed"	real,
	"growthrate_seed_units"	text,
	"growthrate_seed_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "GrowthRateMax" (
	"regions"	text,
	"tech"	text,
	"growthrate_max"	real,
	"growthrate_max_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "GlobalDiscountRate" (
	"rate"	real,
	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
);
CREATE TABLE IF NOT EXISTS "ExistingCapacity" (
	"regions"	text,
	"tech"	text,
	"vintage"	integer,
	"exist_cap"	real,
	"exist_cap_units"	text,
	"exist_cap_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "EmissionLimit" (
	"regions"	text,
	"periods"	integer,
	"emis_comm"	text,
	"emis_limit"	real,
	"emis_limit_units"	text,
	"emis_limit_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","emis_comm"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("emis_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "EmissionActivity" (
	"regions"	text,
	"emis_comm"	text,
	"input_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"output_comm"	text,
	"emis_act"	real,
	"emis_act_units"	text,
	"emis_act_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","emis_comm","input_comm","tech","vintage","output_comm"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("emis_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "Efficiency" (
	"regions"	text,
	"input_comm"	text,
	"tech"	text,
	"vintage"	integer,
	"output_comm"	text,
	"efficiency"	real CHECK("efficiency" > 0),
	"eff_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","input_comm","tech","vintage","output_comm"),
	FOREIGN KEY("output_comm") REFERENCES "commodities"("comm_name"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("input_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "DiscountRate" (
	"regions"	text,
	"tech"	text,
	"vintage"	integer,
	"tech_rate"	real,
	"tech_rate_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "DemandSpecificDistribution" (
	"regions"	text,
	"season_name"	text,
	"time_of_day_name"	text,
	"demand_name"	text,
	"dsd"	real CHECK("dsd" >= 0 AND "dsd" <= 1),
	"dsd_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","season_name","time_of_day_name","demand_name"),
	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),
	FOREIGN KEY("time_of_day_name") REFERENCES "time_of_day"("t_day"),
	FOREIGN KEY("demand_name") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "Demand" (
	"regions"	text,
	"periods"	integer,
	"demand_comm"	text,
	"demand"	real,
	"demand_units"	text,
	"demand_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","demand_comm"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("demand_comm") REFERENCES "commodities"("comm_name")
);
CREATE TABLE IF NOT EXISTS "CostVariable" (
	"regions"	text NOT NULL,
	"periods"	integer NOT NULL,
	"tech"	text NOT NULL,
	"vintage"	integer NOT NULL,
	"cost_variable"	real,
	"cost_variable_units"	text,
	"cost_variable_notes"	text,
	"data_cost_variable" REAL,
	"data_cost_year" INTEGER,
	"data_curr" TEXT,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,

	FOREIGN KEY("data_curr") REFERENCES "currencies"("curr_label"),	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "CostInvest" (
	"regions"	text,
	"tech"	text,
	"vintage"	integer,
	"cost_invest"	real,
	"cost_invest_units"	text,
	"cost_invest_notes"	text,
	"data_cost_invest" REAL,
	"data_cost_year" INTEGER,
	"data_curr" TEXT,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("data_curr") REFERENCES "currencies"("curr_label"),
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "CostFixed" (
	"regions"	text NOT NULL,
	"periods"	integer NOT NULL,
	"tech"	text NOT NULL,
	"vintage"	integer NOT NULL,
	"cost_fixed"	real,
	"cost_fixed_units"	text,
	"cost_fixed_notes"	text,
	"data_cost_fixed" REAL,
	"data_cost_year" INTEGER,
	"data_curr" TEXT,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("data_curr") REFERENCES "currencies"("curr_label"),
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("vintage") REFERENCES "time_periods"("t_periods"),
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods")
);
CREATE TABLE IF NOT EXISTS "CapacityToActivity" (
	"regions"	text,
	"tech"	text,
	"c2a"	real,
	"c2a_notes"	TEXT,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","tech"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "CapacityFactorTech" (
	"regions"	text,
	"season_name"	text,
	"time_of_day_name"	text,
	"tech"	text,
	"cf_tech"	real CHECK("cf_tech" >= 0 AND "cf_tech" <= 1),
	"cf_tech_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","season_name","time_of_day_name","tech"),
	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),
	FOREIGN KEY("time_of_day_name") REFERENCES "time_of_day"("t_day"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech")
);
CREATE TABLE IF NOT EXISTS "CapacityFactorProcess" (
	"regions"	text,
	"season_name"	text,
	"time_of_day_name"	text,
	"tech"	text,
	"vintage"	integer,
	"cf_process"	real CHECK("cf_process" >= 0 AND "cf_process" <= 1),
	"cf_process_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","season_name","time_of_day_name","tech","vintage"),
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),
	FOREIGN KEY("time_of_day_name") REFERENCES "time_of_day"("t_day")
);
CREATE TABLE IF NOT EXISTS "CapacityCredit" (
	"regions"	text,
	"periods"	integer,
	"tech"	text,
	"vintage" integer,
	"cc_tech"	real CHECK("cc_tech" >= 0 AND "cc_tech" <= 1),
	"cc_tech_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	PRIMARY KEY("regions","periods","tech","vintage")
);
CREATE TABLE IF NOT EXISTS "MaxResource" (
	"regions"	text,
	"tech"	text,
	"maxres"	real,
	"maxres_units"	text,
	"maxres_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),
	PRIMARY KEY("regions","tech")
);
CREATE TABLE IF NOT EXISTS "LinkedTechs" (
	"primary_region"	text,
	"primary_tech"	text,
	"emis_comm" text, 
 	"linked_tech"	text,
	"tech_linked_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	FOREIGN KEY("primary_tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("linked_tech") REFERENCES "technologies"("tech"),
	FOREIGN KEY("emis_comm") REFERENCES "commodities"("comm_name"),
	PRIMARY KEY("primary_region","primary_tech", "emis_comm")
);
CREATE TABLE IF NOT EXISTS "MaxSeasonalActivity" (

	"regions"	text,

	"periods"	integer,

	"season_name"	text,

	"tech"	text,

	"maxact"	real,

	"maxact_units"	text,

	"maxact_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),

	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),

	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),

	PRIMARY KEY("regions","periods","season_name","tech")

);
CREATE TABLE IF NOT EXISTS "MinSeasonalActivity" (

	"regions"	text,

	"periods"	integer,

	"season_name"	text,

	"tech"	text,

	"minact"	real,

	"minact_units"	text,

	"minact_notes"	text,

	"reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")
	FOREIGN KEY("periods") REFERENCES "time_periods"("t_periods"),

	FOREIGN KEY("tech") REFERENCES "technologies"("tech"),

	FOREIGN KEY("season_name") REFERENCES "time_season"("t_season"),

	PRIMARY KEY("regions","periods","season_name","tech")

);
CREATE TABLE IF NOT EXISTS "currencies" (

	curr_label text,

	currency_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT currencies_PK PRIMARY KEY (curr_label)

);
CREATE TABLE IF NOT EXISTS "dq_estimate" (

	data_quality_estimated INTEGER,

	dq_est_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_estimate_PK PRIMARY KEY (data_quality_estimated)

);
CREATE TABLE IF NOT EXISTS "dq_completeness" (

	data_quality_completeness INTEGER,

	dq_comp_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_completeness_PK PRIMARY KEY (data_quality_completeness)

);
CREATE TABLE IF NOT EXISTS "dq_reliability" (

	data_quality_reliability INTEGER,

	dq_rel_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_reliability_PK PRIMARY KEY (data_quality_reliability)

);
CREATE TABLE IF NOT EXISTS "dq_time" (

	data_quality_time_related INTEGER,

	dq_time_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_time_PK PRIMARY KEY (data_quality_time_related)

);
CREATE TABLE IF NOT EXISTS "dq_geography" (

	data_quality_geography INTEGER,

	dq_geog_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_geography_PK PRIMARY KEY (data_quality_geography)

);
CREATE TABLE IF NOT EXISTS "dq_technology" (

	data_quality_technology integer,

	dq_tech_description text,

	"reference" text,
	"additional_notes" text,

	FOREIGN KEY("reference") REFERENCES "references"(reference),

	CONSTRAINT dq_technology_PK PRIMARY KEY (data_quality_technology)

);
CREATE TABLE IF NOT EXISTS "references" (

	"reference" text,

	CONSTRAINT references_PK PRIMARY KEY ("reference")
);

CREATE TABLE IF NOT EXISTS "EmissionEmbodied" (
    "regions"     TEXT,
    "emis_comm"   TEXT
        REFERENCES "commodities" (comm_name),
    "tech"        TEXT
        REFERENCES "technologies" (tech),
    "vintage"     INTEGER
        REFERENCES "time_periods" (t_periods),
    "value"       REAL,
    "units"       TEXT,
    "notes"       TEXT,
    "reference" text,
	"data_year" integer,
	"data_flags" text,
	"dq_est" integer,
	"dq_rel" integer,
	"dq_comp" integer,
	"dq_time" integer,
	"dq_geog" integer,
	"dq_tech" integer,
	"additional_notes" text,
	
	FOREIGN KEY("reference") REFERENCES "references"("reference"),
	FOREIGN KEY("dq_est") REFERENCES "dq_estimate"("data_quality_estimated"),
	FOREIGN KEY("dq_rel") REFERENCES "dq_estimate"("data_quality_reliability"),
	FOREIGN KEY("dq_comp") REFERENCES "dq_estimate"("data_quality_completeness"),
	FOREIGN KEY("dq_time") REFERENCES "dq_estimate"("data_quality_time_related"),
	FOREIGN KEY("dq_geog") REFERENCES "dq_estimate"("data_quality_geography"),
	FOREIGN KEY("dq_tech") REFERENCES "dq_estimate"("data_quality_technology")

    PRIMARY KEY ("regions", "emis_comm", "tech", "vintage")
);