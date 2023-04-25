
use `transform_customize`;

select transform_row_id() from (
    select a.order_id, a.site_id, a.amount, a.create_date, b.name from `data/data.json` a join `data/sites.json` b on a.site_id=b.site_id group by a.site_id
);