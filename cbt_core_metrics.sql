

CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_BIDS1');
create table MELI_STAGING.AG_CBT_BIDS1 as (
select	
        a11.SIT_SITE_ID  SIT_SITE_ID,
        a11.TIM_DAY_WINNING_DATE  TIM_DAY,
        c.vertical VERTICAL,
           case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,                                                                               
        coalesce(a11.ITE_TODAY_PROMOTION_FLAG,0) as ITE_TODAY_PROMOTION_FLAG,
        case when promotions.ord_order_id is not null then 1 else 0 end as PROMOTIONS_ATTR,
        
        sum(1)  SUCCESSFULBIDSE,
      	sum((a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK))  GMVE,
      	sum(a11.BID_QUANTITY_OK)  SIE,
      	count(distinct a11.SIT_SITE_ID||to_char(a11.ITE_ITEM_ID))  SLE,
      	
        sum(F_TGMVE) F_TGMVE,
        sum(F_TSIE) F_TSIE,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) end) TGMVE,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_QUANTITY_OK) end) TSIE


        
from	WHOWNER.BTV_BIDS_TRANSACTIONAL_FORECAST a11

	 left outer join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	       on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID
	  
	 left outer join WHOWNER.LK_SELLER_PROMOTIONS_ORDERS promotions
	       on a11.ord_order_id = promotions.ord_order_id
	  
	
       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 
	  
	
	  
where	(a11.TIM_DAY_WINNING_DATE between date -45 and DATE -1       
       and a11.ITE_GMV_FLAG = 1
       and a11.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       and a11.sit_site_id in ('MLA','MLB','MLM','MCO','MPE','MLC','MLU')
       )
 
group by 1,2,3,4,5,6,7,8,9
)
with data primary index (sit_site_id, tim_day, vertical, cbt_Track, holding_group, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;


CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_BIDS_FORECAST');

create table MELI_STAGING.AG_CBT_BIDS_FORECAST as (
select	* from MELI_STAGING.AG_CBT_BIDS1
UNION ALL (select	
        a11.SIT_SITE_ID  SIT_SITE_ID,
        a11.TIM_DAY_WINNING_DATE  TIM_DAY,
        c.vertical VERTICAL,
            case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,  
       coalesce(a11.ITE_TODAY_PROMOTION_FLAG,0) as ITE_TODAY_PROMOTION_FLAG,
       case when promotions.ord_order_id is not null then 1 else 0 end as PROMOTIONS_ATTR,
        
        sum(1)  SUCCESSFULBIDSE,
      	sum((a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK))  GMVE,
      	sum(a11.BID_QUANTITY_OK)  SIE,
      	count(distinct a11.SIT_SITE_ID||to_char(a11.ITE_ITEM_ID))  SLE,

        sum( case when a11.tgmv_flag = 1 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) end) F_TGMVE,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_QUANTITY_OK) end) F_TSIE,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) end) TGMVE,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_QUANTITY_OK) end) TSIE


        
from	WHOWNER.BT_BIDS a11

	 left outer join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID
	  
	 left outer join WHOWNER.LK_SELLER_PROMOTIONS_ORDERS promotions
	  on a11.ord_order_id = promotions.ord_order_id

       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 

	
	
	  
where	(a11.TIM_DAY_WINNING_DATE between DATE -120 and DATE -46       
       and a11.ITE_GMV_FLAG = 1
       and a11.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       and a11.sit_site_id in ('MLA','MLB','MLM','MCO','MPE','MLC','MLU')
       )
 
group by 1,2,3,4,5,6,7,8,9)
        
)
with data primary index (sit_site_id, tim_day, vertical, cbt_Track, holding_group, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;

COLLECT STATISTICS COLUMN (tim_day) ON MELI_STAGING.AG_CBT_BIDS_FORECAST;


--Borra los dias ya insertados 
DELETE FROM TABLEAU_TBL.AG_CBT_BIDS WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.AG_CBT_BIDS_FORECAST);



--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.AG_CBT_BIDS
(sit_site_id,
tim_day,
vertical,
cbt_track,
holding_group,
first_bid_holding,
ORIGIN,
ITE_TODAY_PROMOTION_FLAG,
PROMOTIONS_ATTR,
SUCCESSFULBIDSE,
GMVE,
SIE,
SLE,
F_TGMVE,
F_TSIE,
TGMVE,
TSIE
) 
select stg.sit_site_id, 
       stg.tim_day,
       stg.vertical,
       stg.cbt_track,
       stg.holding_group,
       stg.first_bid_holding,
       stg.origin,
       stg.ITE_TODAY_PROMOTION_FLAG,
       stg.PROMOTIONS_ATTR,
       stg.SUCCESSFULBIDSE,
       stg.GMVE,
       stg.SIE,
       stg.SLE,
       stg.F_TGMVE,
       stg.F_TSIE,
       stg.TGMVE,
       stg.TSIE
from MELI_STAGING.AG_CBT_BIDS_FORECAST as stg;


CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_BIDS_LY_FORECAST');

create table MELI_STAGING.AG_CBT_BIDS_LY_FORECAST as (
select	
        a11.SIT_SITE_ID  SIT_SITE_ID,
        a11.TIM_DAY_WINNING_DATE+364 as TIM_DAY,
        c.vertical,
       
            case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,             
      coalesce(a11.ITE_TODAY_PROMOTION_FLAG,0) as ITE_TODAY_PROMOTION_FLAG,
      case when promotions.ord_order_id is not null then 1 else 0 end as PROMOTIONS_ATTR,
      	
        sum(1)  SUCCESSFULBIDSELY,
      	sum((a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK))  GMVELY,
      	sum(a11.BID_QUANTITY_OK)  SIELY,
      	count(distinct a11.SIT_SITE_ID||to_char(a11.ITE_ITEM_ID))  SLELY,
      	
     -- sum((Case when coalesce(a11.BID_FVF_BONIF, 'Y') = 'N' and COALESCE(a11.auto_offer_flag,0) = 0 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLELY,
     --   sum((Case when coalesce(a11.BID_FVF_BONIF, 'Y') = 'N' and COALESCE(a11.auto_offer_flag,0) = 0 then a11.BID_QUANTITY_OK else 0.0 end))  SIEBILLABLELY

      	

   --   sum((Case when coalesce(a11.BID_FVF_BONIF, 'Y') = 'N' and COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) else 0.0 end))  GMVE_BF,

   -- sum((Case when coalesce(a11.BID_FVF_BONIF, 'Y') = 'N' and COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 then a11.BID_QUANTITY_OK else 0.0 end))  SIE_BF
        
        sum( case when a11.tgmv_flag = 1 then (a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) end) TGMVELY,
        sum( case when a11.tgmv_flag = 1 then (a11.BID_QUANTITY_OK) end) TSIELY

from	WHOWNER.BT_BIDS a11

    left outer join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID
	  
	  left outer join WHOWNER.LK_SELLER_PROMOTIONS_ORDERS promotions
	  on a11.ord_order_id = promotions.ord_order_id

       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 

	  
where	(a11.TIM_DAY_WINNING_DATE between DATE -3-364  and DATE -1-364     
       and a11.ITE_GMV_FLAG = 1
       and a11.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       and a11.sit_site_id in ('MLA','MLB','MLM','MCO','MPE','MLC','MLU')
       )
 
group by 1,2,3,4,5,6,7,8,9
)
with data primary index (sit_site_id, tim_day, vertical, cbt_Track, holding_group, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;


COLLECT STATISTICS COLUMN (tim_day) ON MELI_STAGING.AG_CBT_BIDS_LY_FORECAST;


--Borra los dias ya insertados 
DELETE FROM TABLEAU_TBL.AG_CBT_BIDS_LY WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.AG_CBT_BIDS_LY_FORECAST);


--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.AG_CBT_BIDS_LY
(sit_site_id,
tim_day,
vertical,
cbt_track,
holding_group,
first_bid_holding,
ORIGIN,
ITE_TODAY_PROMOTION_FLAG,
PROMOTIONS_ATTR,
SUCCESSFULBIDSELY,
GMVELY,
SIELY,
SLELY,
TGMVELY,
TSIELY)
select stg.sit_site_id, stg.tim_day,
       stg.vertical,
       stg.cbt_track,
       stg.holding_group,
       stg.first_bid_holding,
       stg.ORIGIN,
       stg.ITE_TODAY_PROMOTION_FLAG,
       stg.PROMOTIONS_ATTR,
       stg.SUCCESSFULBIDSELY,
       stg.GMVELY,
       stg.SIELY,
       stg.SLELY,
       stg.TGMVELY,
       stg.TSIELY
from MELI_STAGING.AG_CBT_BIDS_LY_FORECAST as stg;


--

--LIVELISTINGS
create  volatile table ll_cbt as (
select	
    	a11.SIT_SITE_ID  SIT_SITE_ID,
      a11.TIM_DAY,
      c.vertical VERTICAL,
      
           case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,              
	     sum(a11.LIVELISTINGS)  LIVELISTINGSESELLER
	
from	WHOWNER.BT_LIVE_LISTINGS_SEL	a11

	left outer join	WHOWNER.LK_MKT_MARKETPLACE	a12
	  on 	(a11.CAT_CATEG_ID_L1 = a12.CAT_CATEG_ID)
	  
	inner join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID

       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id_l7 = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 

	  
where	a11.TIM_DAY between DATE -3 and date -1  
       and a12.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       
group by 1,2,3,4,5,6,7
)
with data primary index ( SIT_SITE_ID,TIM_DAY,VERTICAL, CBT_TRACK,ORIGIN,holding_group,first_bid_holding) on commit preserve rows;




--LIVELISTINGS LY
create volatile table ll_cbt_ly as (
select	
    	a11.SIT_SITE_ID  SIT_SITE_ID,
      a11.TIM_DAY+364 as TIM_DAY,
      c.vertical  VERTICAL,
      
          case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,           
	     sum(a11.LIVELISTINGS)  LIVELISTINGSESELLERLY
	
from	WHOWNER.BT_LIVE_LISTINGS_SEL	a11

	left outer join	WHOWNER.LK_MKT_MARKETPLACE	a12
	  on 	(a11.CAT_CATEG_ID_L1 = a12.CAT_CATEG_ID)
	  
	inner join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID

       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id_l7 = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 

	  
where a11.TIM_DAY between DATE -3-364 and date -1-364 
       and a12.MKT_MARKETPLACE_ID = 'TM'
       and a11.photo_id = 'TODATE'
       
group by 1,2,3,4,5,6,7
)
with data primary index ( SIT_SITE_ID,TIM_DAY,VERTICAL, CBT_TRACK,ORIGIN,holding_group,first_bid_holding) on commit preserve rows;





--LIVELISTINGS CBT 

CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_LIVELISTINGS_FORECAST');

create table MELI_STAGING.AG_CBT_LIVELISTINGS_FORECAST as (
select coalesce(a.sit_site_id, b.sit_site_id) as sit_site_id, 
        coalesce(a.tim_day, b.tim_day) as tim_day, 
        coalesce(a.vertical, b.vertical) as vertical, 
        coalesce(a.cbt_Track, b.cbt_Track) as cbt_Track,
         coalesce(a.holding_group, b.holding_group) as holding_group, 
        coalesce(a.first_bid_holding, b.first_bid_holding) as first_bid_holding, 
        coalesce(a.ORIGIN, b.ORIGIN) as ORIGIN,
        null as ITE_TODAY_PROMOTION_FLAG,
        null as PROMOTIONS_ATTR,
        LIVELISTINGSESELLER,
        LIVELISTINGSESELLERLY
from ll_cbt as a
    full outer join
    ll_cbt_ly as b
    on a.sit_site_id = b.sit_site_id
    and a.tim_day = b.tim_Day
    and a.vertical = b.vertical
    and a.cbt_Track = b.cbt_track
    and a.origin = b.origin
    and a.holding_group = b.holding_group)
with data primary index (sit_site_id, tim_day, vertical, cbt_Track, holding_group, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;

COLLECT STATISTICS COLUMN (tim_day) ON MELI_STAGING.AG_CBT_LIVELISTINGS_FORECAST;


--Borra los dias ya insertados 
DELETE FROM TABLEAU_TBL.AG_CBT_LIVELISTINGS WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.AG_CBT_LIVELISTINGS_FORECAST);


--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.AG_CBT_LIVELISTINGS
(sit_site_id,
tim_day,
vertical,
cbt_track,
holding_group,
first_bid_holding,
ORIGIN,
ITE_TODAY_PROMOTION_FLAG,
PROMOTIONS_ATTR,
LIVELISTINGSESELLER,
LIVELISTINGSESELLERLY)
select stg.sit_site_id, stg.tim_day,
       stg.vertical,
       stg.cbt_track,
       stg.holding_group,
       stg.first_bid_holding,
       stg.ORIGIN,
       stg.ITE_TODAY_PROMOTION_FLAG,
       stg.PROMOTIONS_ATTR,
       stg.LIVELISTINGSESELLER,
       stg.LIVELISTINGSESELLERLY
from MELI_STAGING.AG_CBT_LIVELISTINGS_FORECAST as stg;



--VISITS CBT
create volatile table visits_cbt as (
select	

    a11.SIT_SITE_ID  SIT_SITE_ID,
	  a11.TIM_DAY  TIM_DAY,	
	
	c.vertical  VERTICAL,

          case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,         

	  sum(a11.QTY_VISITS)  VISITSVIPE
	
	
from	WHOWNER.BT_VISITS_VIP	a11

	left outer join	WHOWNER.LK_MKT_MARKETPLACE	a12
	  on 	(a11.CAT_CATEG_ID_L1 = a12.CAT_CATEG_ID)
	  
  inner join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID

       left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id_l7 = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 

	  
where	a11.TIM_DAY between date -3 and date-1      
       and a12.MKT_MARKETPLACE_ID = 'TM'
 
group by 1,2,3,4,5,6,7
)
with data primary index ( SIT_SITE_ID,TIM_DAY,VERTICAL, CBT_TRACK, holding_group,ORIGIN) on commit preserve rows;





--VISITS CBT LY
create volatile table visits_cbt_ly as (
select	

    a11.SIT_SITE_ID  SIT_SITE_ID,
	  a11.TIM_DAY+364  TIM_DAY,	
	c.vertical  VERTICAL,
    case when cbt.cus_cbt_flag_t1 + cbt.cus_cbt_flag_t2+cbt.cus_cbt_flag_t3+cbt.cus_cbt_flag_t4 = 1 then
                case when cbt.cus_cbt_flag_t1 = 1 then 1
                     when cbt.cus_cbt_flag_t2 = 1 then 2
                     when cbt.cus_cbt_flag_t3 = 1 then 3
                     when cbt.cus_cbt_flag_t4 = 1 then 4
                end
         else 0 end as CBT_TRACK,
        coalesce(cbt.cus_cbt_holding_group,'N/A') as holding_group,
        cbt.first_bid_date_holding as first_bid_holding,
       coalesce(cbt.ITE_CBT_ORIGIN,'N/A') as origin,              
 sum(a11.QTY_VISITS)  VISITSVIPELY
	
	
from	WHOWNER.BT_VISITS_VIP	a11

	left outer join	WHOWNER.LK_MKT_MARKETPLACE	a12
	  on 	(a11.CAT_CATEG_ID_L1 = a12.CAT_CATEG_ID)
	 
  inner join		WHOWNER.LK_CUS_CBT_ITEM_ORIGIN	cbt
	  on 	a11.CUS_CUST_ID_SEL = cbt.CUS_CUST_ID

         left join WHOWNER.AG_LK_CAT_CATEGORIES_PH c 
              on a11.cat_categ_id_l7 = c.cat_categ_id_l7 and c.photo_id = 'TODATE' and a11.sit_site_id = c.sit_site_id 


where	a11.TIM_DAY between DATE -3-364 and date-1-364    
       and a12.MKT_MARKETPLACE_ID = 'TM'
 
group by 1,2,3,4,5,6,7
)
with data primary index ( SIT_SITE_ID,TIM_DAY,VERTICAL, CBT_TRACK,holding_group,ORIGIN ) on commit preserve rows;






--VISITS CBT TOTAL

CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_VISITS_FORECAST');

create table MELI_STAGING.AG_CBT_VISITS_FORECAST as (
select coalesce(a.sit_site_id, b.sit_site_id) as sit_site_id, 
        coalesce(a.tim_day, b.tim_day) as tim_day, 
        coalesce(a.vertical, b.vertical) as vertical, 
        coalesce(a.cbt_Track, b.cbt_Track) as cbt_Track,
        coalesce(a.holding_group, b.holding_group) as holding_group, 
        coalesce(a.first_bid_holding,b.first_bid_holding) as first_bid_holding,
        coalesce(a.ORIGIN, b.ORIGIN) as ORIGIN, 
        null as ITE_TODAY_PROMOTION_FLAG,
        null as PROMOTIONS_ATTR,
        VISITSVIPE,
        VISITSVIPELY
from visits_cbt as a
    full outer join
    visits_cbt_ly as b
    on a.sit_site_id = b.sit_site_id
    and a.tim_day = b.tim_Day
    and a.vertical = b.vertical
    and a.cbt_Track = b.cbt_track
    and a.ORIGIN = b.ORIGIN
    and a.holding_group=b.holding_group)
with data primary index (sit_site_id, tim_day, vertical, cbt_Track, holding_group, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;


COLLECT STATISTICS COLUMN (tim_day) ON MELI_STAGING.AG_CBT_VISITS_FORECAST;



--Borra los dias ya insertados 
DELETE FROM TABLEAU_TBL.AG_CBT_VISITS WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.AG_CBT_VISITS_FORECAST);



--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.AG_CBT_VISITS
(sit_site_id,
tim_day,
vertical,
cbt_track,
holding_group,
first_bid_holding,
ORIGIN,
ITE_TODAY_PROMOTION_FLAG,
PROMOTIONS_ATTR,
VISITSVIPE,
VISITSVIPELY)
select stg.sit_site_id, 
       stg.tim_day,
       stg.vertical,
       stg.cbt_track,
       stg.holding_group,
       stg.first_bid_holding,
       stg.ORIGIN,
       stg.ITE_TODAY_PROMOTION_FLAG,
       stg.PROMOTIONS_ATTR,
       stg.VISITSVIPE,
       stg.VISITSVIPELY
from MELI_STAGING.AG_CBT_VISITS_FORECAST as stg;








CALL MELI_LIB.PR_DROP_TABLE('MELI_STAGING','AG_CBT_METRICS_FORECAST');

create table MELI_STAGING.AG_CBT_METRICS_FORECAST as (

select coalesce(d.sit_site_id,coalesce(c.sit_site_id,coalesce(a.sit_site_id, b.sit_site_id))) as sit_site_id, 
        coalesce(d.tim_day,coalesce(c.tim_day,coalesce(a.tim_day, b.tim_day))) as tim_day, 
        coalesce(d.vertical,coalesce(c.vertical,coalesce(a.vertical, b.vertical))) as vertical, 
        coalesce(d.cbt_track,coalesce(c.cbt_Track,coalesce(a.cbt_Track, b.cbt_Track))) as cbt_track,
        coalesce(d.holding_group,coalesce(c.holding_group,coalesce(a.holding_group, b.holding_group))) as holding_group,
        coalesce(d.first_bid_holding,coalesce(c.first_bid_holding,coalesce(a.first_bid_holding, b.first_bid_holding))) as first_bid_holding,
        coalesce(d.ORIGIN,coalesce(c.ORIGIN,coalesce(a.ORIGIN, b.ORIGIN))) as origin,  
        coalesce(d.ITE_TODAY_PROMOTION_FLAG,coalesce(c.ITE_TODAY_PROMOTION_FLAG,coalesce(a.ITE_TODAY_PROMOTION_FLAG, b.ITE_TODAY_PROMOTION_FLAG))) as ITE_TODAY_PROMOTION_FLAG,
        coalesce(d.PROMOTIONS_ATTR,coalesce(c.PROMOTIONS_ATTR,coalesce(a.PROMOTIONS_ATTR, b.PROMOTIONS_ATTR))) as PROMOTIONS_ATTR,
        
        GMVE,SIE,SUCCESSFULBIDSE,SLE, F_TGMVE, F_TSIE, TGMVE, TSIE,   --BIDS
        b.GMVELY,b.SIELY,b.SUCCESSFULBIDSELY,b.SLELY, b.TGMVELY, b.TSIELY,--BIDS LY
        c.VISITSVIPE,c.VISITSVIPELY,  --VISITS
        d.LIVELISTINGSESELLER, d.LIVELISTINGSESELLERLY --LIVELISTINGS
        
        
from TABLEAU_TBL.AG_CBT_BIDS as a
    
          full outer join
          TABLEAU_TBL.AG_CBT_BIDS_LY as b
          on a.sit_site_id = b.sit_site_id
          and a.tim_day = b.tim_Day
          and a.vertical = b.vertical
          and a.cbt_Track = b.cbt_track
          and a.holding_group = b.holding_group
          and a.origin = b.origin
          and a.ITE_TODAY_PROMOTION_FLAG = b.ITE_TODAY_PROMOTION_FLAG
          and a.PROMOTIONS_ATTR = b.PROMOTIONS_ATTR
          
                    full outer join
          TABLEAU_TBL.AG_CBT_VISITS as c
          on a.sit_site_id = c.sit_site_id
          and a.tim_day = c.tim_Day
          and a.vertical = c.vertical
          and a.cbt_Track = c.cbt_track
          and a.holding_group = c.holding_group
          and a.origin = c.origin
          and a.ITE_TODAY_PROMOTION_FLAG = c.ITE_TODAY_PROMOTION_FLAG
          and a.PROMOTIONS_ATTR = c.PROMOTIONS_ATTR
          
           full outer join
          TABLEAU_TBL.AG_CBT_LIVELISTINGS as d 
          on a.sit_site_id = d.sit_site_id
          and a.tim_day = d.tim_Day
          and a.vertical = d.vertical
          and a.cbt_Track = d.cbt_track
          and a.holding_group = d.holding_group
          and a.origin = d.origin
          and a.ITE_TODAY_PROMOTION_FLAG = d.ITE_TODAY_PROMOTION_FLAG
          and a.PROMOTIONS_ATTR = d.PROMOTIONS_ATTR
                   
WHERE A.TIM_DAY >= DATE -120  OR B.TIM_DAY >= DATE -120 or c.tim_day >= date -120   or D.TIM_DAY >= DATE -120


)with data primary index (sit_site_id, tim_day, vertical, cbt_Track, ORIGIN, ITE_TODAY_PROMOTION_FLAG, PROMOTIONS_ATTR) ;


COLLECT STATISTICS COLUMN (tim_day) ON MELI_STAGING.AG_CBT_METRICS_FORECAST;



--Borra los dias ya insertados de la tabla final
DELETE FROM TABLEAU_TBL.AG_CBT_METRICS WHERE tim_day IN 
(SELECT DISTINCT 
      (tim_day) 
FROM MELI_STAGING.AG_CBT_METRICS_FORECAST);


--Inserta a la tabla productiva los dias nuevos
INSERT INTO TABLEAU_TBL.AG_CBT_METRICS
(sit_site_id,
tim_day,
vertical,
cbt_track,
holding_group,
first_bid_holding,
origin,
ITE_TODAY_PROMOTION_FLAG,
PROMOTIONS_ATTR,
GMVE,
SIE,
F_TGMVE, 
F_TSIE, 
TGMVE, 
TSIE,
SUCCESSFULBIDSE,
SLE, 
GMVELY,
SIELY,
SUCCESSFULBIDSELY,
SLELY,
TGMVELY, 
TSIELY,
VISITSVIPE,
VISITSVIPELY,
LIVELISTINGSESELLER, 
LIVELISTINGSESELLERLY)
select 
       stg.sit_site_id, 
       stg.tim_day,
       stg.vertical,
       stg.cbt_track,
       stg.holding_group,
       stg.first_bid_holding,
       stg.origin,
       stg.ITE_TODAY_PROMOTION_FLAG,
       stg.PROMOTIONS_ATTR,
       stg.GMVE,
       stg.SIE,
       stg.F_TGMVE, 
       stg.F_TSIE, 
       stg.TGMVE, 
       stg.TSIE, 
       stg.SUCCESSFULBIDSE,
       stg.SLE,
       stg.GMVELY,
       stg.SIELY,
       stg.SUCCESSFULBIDSELY,
       stg.SLELY,
       stg.TGMVELY, 
       stg.TSIELY,
       stg.VISITSVIPE,
       stg.VISITSVIPELY,
      stg.LIVELISTINGSESELLER, 
      stg.LIVELISTINGSESELLERLY
from MELI_STAGING.AG_CBT_METRICS_FORECAST as stg;
