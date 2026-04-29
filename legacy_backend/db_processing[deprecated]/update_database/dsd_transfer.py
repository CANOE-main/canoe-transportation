import sqlite3
import os

target_name = 'canoe_on_12d_vanilla_nhts_fixed'
source_name = 'canoe_trn_vanilla_nhts_v3'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
target = dir_path + 'target_database/' + target_name + '.sqlite'  # Database to be updated (12D vanilla)
source = dir_path + '../to_temoa_v3/v3_database/' + source_name + '.sqlite'  # Where new datapoints come from

def update_dsd_values(target, source, demand_names):
    # Connect to both databases
    conn_small = sqlite3.connect(target)
    cursor_small = conn_small.cursor()
    conn_large = sqlite3.connect(source)
    cursor_large = conn_large.cursor()

    # Iterate over the demand names to update
    for demand_name in demand_names:
        # Fetch matching records from the large database
        cursor_large.execute(
            """
            SELECT season, tod, dsd
            FROM DemandSpecificDistribution
            WHERE demand_name = ?
            """, (demand_name,)
        )
        large_db_records = cursor_large.fetchall()

        # Update the small database with values from the large database
        for season, tod, dsd in large_db_records:
            cursor_small.execute(
                """
                UPDATE DemandSpecificDistribution
                SET dsd = ?
                WHERE demand_name = ? AND season = ? AND tod = ?
                """, (dsd, demand_name, season, tod)
            )

        # Normalize dsd values to ensure they sum to 1
        cursor_small.execute(
            """
            SELECT season, tod, dsd
            FROM DemandSpecificDistribution
            WHERE demand_name = ?
            """, (demand_name,)
        )
        updated_records = cursor_small.fetchall()

        # Calculate the total sum of dsd values for normalization
        total_sum = sum(dsd for _, _, dsd in updated_records)

        # Update each dsd value to normalize
        if total_sum != 0:
            for season, tod, dsd in updated_records:
                normalized_dsd = dsd / total_sum
                cursor_small.execute(
                    """
                    UPDATE DemandSpecificDistribution
                    SET dsd = ?
                    WHERE demand_name = ? AND season = ? AND tod = ?
                    """, (normalized_dsd, demand_name, season, tod)
                )

    # Commit changes and close connections
    conn_small.commit()
    conn_small.close()
    conn_large.close()

# Define the demand names to update
demand_names = ['T_D_pkm_ldv_c', 'T_D_pkm_ldv_t', 'T_D_tkm_ldv_t']

# Run the update function
update_dsd_values(target, source, demand_names)
