with raw_data as (
    SELECT
    VALUE:id::integer as id
    ,VALUE:name::string as name
    ,VALUE:slug::string as slug
    ,VALUE:website::string as website
    ,VALUE:teamSize::integer as teamSize
    ,VALUE:url::string as url
    ,VALUE:batch::string as batch
    ,VALUE:tags::array(string) as tags
    ,VALUE:status::string as status
    ,VALUE:industries::array(string) as industries
    ,VALUE:regions::array(string) as regions
    ,VALUE:locations::array(string) as locations
    FROM {{ref("base_raw_data_y_combinator_raw_data")}}, table(flatten(json)))
select * from raw_data