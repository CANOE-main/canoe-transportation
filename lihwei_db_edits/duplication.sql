-- This is SQL code to duplicate rows from an sqlite database, renaming certain columns

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