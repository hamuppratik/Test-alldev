import pandas as pd
import numpy as np

def add_proc_code_flag(df):
    """
    Adds 'proc_code_flag' and 'target_code' columns to the input DataFrame using
    duplicate detection logic based on procedure codes, modifiers, quantity, and dates.
    """
    # Data preparation
    df['plan_paid_amount'] = df['plan_paid_amount'].astype(float)
    df['claim_received_date'] = pd.to_datetime(df['claim_received_date'], errors='coerce')

    modifier_cols = ['proc_modifier', 'proc_modifier2', 'proc_modifier4', 'proc_modifier5']
    df['procedure_code'] = df['procedure_code'].str.strip()
    df[modifier_cols] = df[modifier_cols].fillna('').apply(lambda x: x.str.strip().str.lower())
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    # Grouping keys
    group_keys = [
        'member_medicare_id',
        'procedure_code',
        'proc_modifier',
        'proc_modifier2',
        'proc_modifier4',
        'proc_modifier5',
        'quantity'
    ]

    # Flagging logic per group
    def flag_by_procedure(group):
        group = group[group['plan_paid_amount'] > 75].sort_values('claim_received_date')
        if group.empty or len(group) == 1:
            return pd.Series(['other'] * len(group), index=group.index)

        first_date = group.iloc[0]['claim_received_date']
        last_date = group.iloc[-1]['claim_received_date']

        flags = []
        for _, row in group.iterrows():
            if first_date == last_date:
                flags.append('duplicate')
            elif row['claim_received_date'] == first_date:
                flags.append('reference')
            elif row['claim_received_date'] == last_date:
                flags.append('target')
            else:
                flags.append('other')
        return pd.Series(flags, index=group.index)

    # Apply flagging
    flags_series = df.groupby(group_keys, group_keys=False).apply(flag_by_procedure)
    df['proc_code_flag'] = 'other'
    df.loc[flags_series.index, 'proc_code_flag'] = flags_series

    # Add target_code
    df['target_code'] = df.apply(
        lambda row: row['procedure_code'] if row['proc_code_flag'] in ['target', 'duplicate'] else np.nan,
        axis=1
    )

    return df
