-- This is SQL code to apply modifications to an sqlite database

/*DUPLICATIONS
tech mapping:
T_HDV_T_DSL_EX: 
    T_HDV_T_LH_DSL_EX
T_HDV_T_DSL_N:
    T_HDV_T_LH_DSL_N
T_HDV_T_BEV_N: 
    T_HDV_T_LH_BEV_LRGBATT_N
    T_HDV_T_LH_BEV_FSTCHRG_N
    T_HDV_T_LH_BEV_ERDS1_N
    T_HDV_T_LH_BEV_ERDS2_N
    T_HDV_T_LH_BEV_ERDS3_N
    T_HDV_T_LH_BEV_ERDS4_N
    T_HDV_T_LH_BEV_ERDS5_N
T_HDV_T_FCEV_N:
    T_HDV_T_LH_FCEV_N
T_HDV_CHRG:
    T_HDV_T_LH_CHRG_LRGBATT
    T_HDV_T_LH_CHRG_FSTCHRG
T_H2_HDV_REFUEL:
    T_H2_HDV_T_LH_REFUEL
-----
*/

/*
DELETIONS
- Demand: remove rows where commodity = 'T_D_tkm_hdv_t' and region = ON
- ExistingCapacity: remove rows where tech = 'T_HDV_T_DSL_EX' and region = ON
- MaxAnnualCapacityFactor, MinAnnualCapacityFactor: remove rows where tech is 'T_HDV_T_(DSL|BEV|FC)' and region = ON
- Efficiency: remove rows where tech is 'T_HDV_T_(DSL|BEV|FC)' and region = ON
- CostInvest: remove rows where tech is 'T_HDV_T_(DSL|BEV|FC)' and region = ON
*/
DELETE FROM Demand WHERE commodity = 'T_D_tkm_hdv_t' AND region = 'ON';
DELETE FROM ExistingCapacity WHERE tech = 'T_HDV_T_DSL_EX' AND region = 'ON';
DELETE FROM MaxAnnualCapacityFactor WHERE (tech LIKE 'T_HDV_T_DSL%' OR tech LIKE 'T_HDV_T_BEV%' OR tech LIKE 'T_HDV_T_FC%') AND region = 'ON';
DELETE FROM MinAnnualCapacityFactor WHERE (tech LIKE 'T_HDV_T_DSL%' OR tech LIKE 'T_HDV_T_BEV%' OR tech LIKE 'T_HDV_T_FC%') AND region = 'ON';
DELETE FROM Efficiency WHERE (tech LIKE 'T_HDV_T_DSL%' OR tech LIKE 'T_HDV_T_BEV%' OR tech LIKE 'T_HDV_T_FC%') AND region = 'ON';
DELETE FROM CostInvest WHERE (tech LIKE 'T_HDV_T_DSL%' OR tech LIKE 'T_HDV_T_BEV%' OR tech LIKE 'T_HDV_T_FC%') AND region = 'ON';

/*
MODIFICATIONS
- CostVariable: Make copies of rows where tech = T_HDV_T_DSL_EX, T_HDV_T_DSL_N, T_HDV_T_FCEV_N, T_HDV_T_BEV_N and region = ON. Change tech from T_HDV_T_ to T_HDV_T_LH_.
*/

-- In CostVariable, make copies of T_HDV_T_... and make it T_HDV_T_LH_...

INSERT INTO CostVariable (tech, region, period, vintage, units, cost, notes, reference)
SELECT 
    REPLACE(tech, 'T_HDV_T_', 'T_HDV_T_LH_') AS tech,
    region, period, vintage, units, cost, notes, reference
FROM CostVariable
WHERE tech IN ('T_HDV_T_DSL_EX', 'T_HDV_T_DSL_N', 'T_HDV_T_FCEV_N', 'T_HDV_T_BEV_N')
  AND region = 'ON';

-- Make copies of rows where tech = 'T_HDV_T_LH_BEV_N' for each new tech variant
-- base holds all rows where tech = 'T_HDV_T_LH_BEV_N' and region = 'ON'
-- tech_names is a virtual table of the new tech names.
WITH base AS (
    SELECT *
    FROM CostVariable
    WHERE tech = 'T_HDV_T_LH_BEV_N' and region = 'ON'
), 
tech_names(name) AS (
    VALUES
        ('T_HDV_T_LH_BEV_LRGBATT_N'),
        ('T_HDV_T_LH_BEV_FSTCHRG_N'),
        ('T_HDV_T_LH_BEV_ERDS1_N'),
        ('T_HDV_T_LH_BEV_ERDS2_N'),
        ('T_HDV_T_LH_BEV_ERDS3_N'),
        ('T_HDV_T_LH_BEV_ERDS4_N'),
        ('T_HDV_T_LH_BEV_ERDS5_N')
)
INSERT INTO CostVariable (
    tech, region, period, vintage, units, cost, notes, reference
)
SELECT 
    t.name AS tech,
    b.region, b.period, b.vintage, b.units, b.cost, b.notes, b.reference
FROM base b
JOIN tech_names t ON 1=1;
DELETE FROM CostVariable WHERE tech = 'T_HDV_T_LH_BEV_N' and region = 'ON';
