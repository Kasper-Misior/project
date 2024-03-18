with

get_company_total_headcount as (
    select
        uuid,
        name,
        employee_count
    from {{ref("base_crunchbase_crunchbase_organizations")}}
    where employee_count is not null
        and employee_count!='unknown'
        and name is not null
        and status='operating'),

get_company_engineering_headcount as (
    select
        crunchbase_company_uuid,
        count(*) as engineering_employees_count
    from {{ref("base_crunchbase_positions_latest")}}
    where startdate is not null 
        and enddate is null
        and role_k7 = 'Engineer'
    group by crunchbase_company_uuid
),

get_both_headcounts as (
    select
        get_company_total_headcount.uuid,
        get_company_total_headcount.name,
        get_company_total_headcount.employee_count,
        get_company_engineering_headcount.engineering_employees_count
    from get_company_total_headcount
    inner join get_company_engineering_headcount
        on get_company_total_headcount.uuid = get_company_engineering_headcount.crunchbase_company_uuid),

get_raw as (
    select
        *
    from {{ref("raw_data")}}),

get_money_from_ipos as (
    select
        org_name,
        sum(money_raised_usd) as ipo_money_raised_usd
    from {{ref("base_crunchbase_crunchbase_ipos")}}
    where money_raised_usd is not null
    group by org_name),

get_money_from_funding as (
    select
        org_name,
        sum(raised_amount_usd) as funding_raised_amount_usd,
        max(announced_on) as latest_founding_date
    from {{ref("base_crunchbase_crunchbase_funding_rounds")}}
    where raised_amount_usd is not null
    group by org_name),
    
get_sum_raised as (
select
    get_money_from_ipos.org_name,
    get_money_from_ipos.ipo_money_raised_usd+get_money_from_funding.funding_raised_amount_usd as total_sum_raised,
    get_money_from_funding.latest_founding_date
    from get_money_from_ipos
    inner join get_money_from_funding
        on get_money_from_ipos.org_name = get_money_from_funding.org_name)
        
select
    get_both_headcounts.uuid,
    get_both_headcounts.name,
    get_raw.locations,
    get_both_headcounts.employee_count,
    get_both_headcounts.engineering_employees_count,
    get_sum_raised.latest_founding_date,
    get_sum_raised.total_sum_raised,
    get_raw.batch,
    get_raw.website

from get_both_headcounts
inner join get_raw
    on get_both_headcounts.name = get_raw.name
inner join get_sum_raised
    on get_both_headcounts.name = get_sum_raised.org_name