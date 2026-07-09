-- mart_metric_dictionary: Governed metric definitions.
-- One row per metric. This is the single source of truth for what every
-- financial measure means, how it's calculated, and where it's used.
-- Referenced by AI narrative chains and Power BI tooltips.

{{ config(materialized='table') }}

select * from (values
    ('revenue',            'Revenue',                'sum(amount_inr) where account_type = ''Revenue''',                      'INR', 'mart_pnl_summary',      'P&L Summary, Revenue Trends'),
    ('cogs',               'Cost of Goods Sold',     'sum(amount_inr) where account_type = ''COGS'' (stored negative)',       'INR', 'mart_pnl_summary',      'P&L Summary, Cost Analysis'),
    ('gross_margin',       'Gross Margin',           'revenue + cogs (cogs is negative, so effectively revenue - |cogs|)',    'INR', 'mart_pnl_summary',      'P&L Summary, Variance Analysis'),
    ('gross_margin_pct',   'Gross Margin %',         '(revenue + cogs) / revenue * 100',                                     '%',   'mart_pnl_summary',      'P&L Summary'),
    ('opex',               'Operating Expenses',     'sum(amount_inr) where account_type = ''OpEx'' (stored negative)',       'INR', 'mart_pnl_summary',      'P&L Summary, Cost Analysis'),
    ('operating_income',   'Operating Income',       'revenue + cogs + opex',                                                 'INR', 'mart_pnl_summary',      'P&L Summary, Variance Analysis'),
    ('operating_margin_pct','Operating Margin %',    '(revenue + cogs + opex) / revenue * 100',                               '%',   'mart_pnl_summary',      'P&L Summary'),
    ('revenue_mom_change', 'Revenue MoM Change %',   '(current - prior) / |prior| * 100 partitioned by segment+project',     '%',   'mart_revenue_trends',   'Revenue Trends'),
    ('cost_mom_change',    'Cost MoM Change %',      '(current - prior) / prior * 100 partitioned by segment+cc+account',    '%',   'mart_cost_analysis',    'Cost Analysis'),
    ('oi_variance_pct',    'OI Variance %',          '(current OI - prior OI) / |prior OI| * 100',                           '%',   'mart_variance_analysis','Variance Analysis'),
    ('is_material_variance','Material Variance Flag', 'true when |oi_variance_pct| > 10%',                                   'bool','mart_variance_analysis','Variance Analysis')
) as t(metric_key, metric_name, calculation_logic, unit, source_model, used_in_views)