create or replace view openpowerlifting.v_lifter as 
with t as (
    select 
        *,
        max(created_date) over () as max_created_date
    from openpowerlifting.lifter
)
select 
    name,
    sex,
    event,
    equipment,
    age,
    ageclass,
    birthyearclass,
    division,
    bodyweightkg,
    weightclasskg,
    squat1kg,
    squat2kg,
    squat3kg,
    squat4kg,
    best3squatkg,
    bench1kg,
    bench2kg,
    bench3kg,
    bench4kg,
    best3benchkg,
    deadlift1kg,
    deadlift2kg,
    deadlift3kg,
    deadlift4kg,
    best3deadliftkg,
    totalkg,
    place,
    dots,
    wilks,
    glossbrenner,
    goodlift,
    tested,
    country,
    state,
    federation,
    parentfederation,
    date,
    meetcountry,
    meetstate,
    meettown,
    meetname,
    sanctioned,
    downloaded_at,
    created_date
from t 
where 
    created_date = max_created_date