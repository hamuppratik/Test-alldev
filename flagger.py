
import pandas as pd
import numpy as np

def add_proc_code_flag(df):
    """
    Adds a column 'proc_code_flag' to the input DataFrame based on duplicate procedure code logic.
    """

    # Ensure correct data types and formatting
    df['plan_paid_amount'] = df['plan_paid_amount'].astype(float)
    df['claim_received_date'] = pd.to_datetime(df['claim_received_date'], errors='coerce')

    modifier_cols = ['proc_modifier', 'proc_modifier2', 'proc_modifier4', 'proc_modifier5']
    df['procedure_code'] = df['procedure_code'].str.strip()
    df[modifier_cols] = df[modifier_cols].fillna('').apply(lambda x: x.str.strip().str.lower())
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    # Group keys
    group_keys = [
        'member_medicare_id',
        'procedure_code',
        'proc_modifier',
        'proc_modifier2',
        'proc_modifier4',
        'proc_modifier5',
        'quantity'
    ]

    # Group function
    def flag_by_procedure(group):
        group = group[group['plan_paid_amount'] > 75].sort_values('claim_received_date')
        if group.shape[0] > 1:
            group['proc_code_flag'] = 1
        else:
            group['proc_code_flag'] = 0
        return group

    # Apply group function
    df = df.groupby(group_keys, group_keys=False).apply(flag_by_procedure)

    # Fill missing flags with 0
    if 'proc_code_flag' not in df.columns:
        df['proc_code_flag'] = 0
    else:
        df['proc_code_flag'] = df['proc_code_flag'].fillna(0)

    df['proc_code_flag'] = df['proc_code_flag'].astype(int)
    return df
